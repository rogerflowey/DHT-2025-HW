package naive

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

// MiniNode represents a single node in the DHT network.
type MiniNode struct {
	Addr   string // address and port number of the node, e.g., "localhost:1234"
	online bool

	listener net.Listener
	server   *rpc.Server

	poolMutex  sync.RWMutex
	clientPool map[string]*rpc.Client

	activeConnMutex sync.Mutex
	activeConns     map[net.Conn]struct{}
}

func (node *MiniNode) Init(addr string) {
	node.Addr = addr
	node.clientPool = make(map[string]*rpc.Client)
	node.activeConns = make(map[net.Conn]struct{})
}

// RunRPCServer now takes an argument: the object whose methods should be exposed via RPC.
func (node *MiniNode) RunRPCServer(rcvr interface{}, wg *sync.WaitGroup) {
	node.server = rpc.NewServer()

	// THE KEY CHANGE:
	// Instead of registering `node` (the MiniNode), we register the `rcvr` object passed to us.
	// This will automatically find and register all valid public methods on the receiver.
	err := node.server.Register(rcvr)
	if err != nil {
		logrus.Fatalf("[%s] Failed to register RPC receiver: %v", node.Addr, err)
	}

	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done()
	if err != nil {
		logrus.Fatalf("[%s] Listen error: %v", node.Addr, err)
	}

	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			if node.online {
				logrus.Errorf("[%s] Accept error: %v", node.Addr, err)
			}
			return
		}

		node.activeConnMutex.Lock()
		node.activeConns[conn] = struct{}{}
		node.activeConnMutex.Unlock()

		go func(c net.Conn) {
			// --- THE FIX: When ServeConn finishes, untrack the connection ---
			defer func() {
				node.activeConnMutex.Lock()
				delete(node.activeConns, c)
				node.activeConnMutex.Unlock()
				c.Close() // Ensure it's closed
			}()
			node.server.ServeConn(c)
		}(conn)
	}
}

// The Run method also needs to be updated to pass the receiver.
func (node *MiniNode) Run(rcvr interface{}, wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(rcvr, wg)
}

func (node *MiniNode) StopRPCServer() {
	if !node.online {
		return
	}
	node.online = false
	if node.listener != nil {
		node.listener.Close()
	}

	node.activeConnMutex.Lock()
	defer node.activeConnMutex.Unlock()

	logrus.Warnf("[%s] Shutting down. Closing %d active server connections.", node.Addr, len(node.activeConns))
	for conn := range node.activeConns {
		// This will cause the corresponding ServeConn goroutine to get an
		// error and exit.
		conn.Close()
	}
}

var (
	ErrNetwork     = errors.New("network error")
	ErrApplication = errors.New("application error")
)

// getClient retrieves a client from the pool or creates a new one.
// This is the core of the connection pooling logic.
func (node *MiniNode) getClient(addr string) (*rpc.Client, error) {
	// First, try with a read lock for high concurrency.
	node.poolMutex.RLock()
	client, ok := node.clientPool[addr]
	node.poolMutex.RUnlock()

	if ok && client != nil {
		return client, nil // Found an existing client, reuse it.
	}

	// If no client was found, we need a write lock to create one.
	node.poolMutex.Lock()
	defer node.poolMutex.Unlock()

	// It's possible another goroutine created the client while we were waiting for the lock.
	// This is the "double-checked locking" pattern.
	client, ok = node.clientPool[addr]
	if ok && client != nil {
		return client, nil
	}

	// Create a new connection.
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		logrus.Errorf("[%s] Dialing %s error: %v", node.Addr, addr, err)
		return nil, fmt.Errorf("%w: failed to dial %s: %v", ErrNetwork, addr, err)
	}

	// Create a new RPC client and store it in the pool.
	client = rpc.NewClient(conn)
	node.clientPool[addr] = client

	logrus.Infof("[%s] Created new pooled connection to %s", node.Addr, addr)
	return client, nil
}

// removeClient closes and removes a dead client from the pool.
func (node *MiniNode) removeClient(addr string) {
	node.poolMutex.Lock()
	defer node.poolMutex.Unlock()

	client, ok := node.clientPool[addr]
	if ok && client != nil {
		client.Close() // Close the underlying connection.
		delete(node.clientPool, addr)
		logrus.Warnf("[%s] Removed dead client for %s from pool", node.Addr, addr)
	}
}

// RemoteCall uses the connection pool to make an RPC call.
func (node *MiniNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "MiniNode.Ping" {
		logrus.Infof("[%s] RemoteCall -> %s method %s with args %v", node.Addr, addr, method, args)
	}

	// Get a client from our pool.
	client, err := node.getClient(addr)
	if err != nil {
		// Error is already logged in getClient, just return it.
		return err
	}

	// Make the RPC call using the pooled client.
	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Errorf("[%s] RemoteCall to %s method %s error: %v", node.Addr, addr, method, err)
		// If the error is rpc.ErrShutdown, it means the connection is closed/broken.
		// We should remove this dead client from our pool.
		// Handle network-related errors that indicate a broken connection.
		if err == rpc.ErrShutdown ||
			err == io.EOF ||
			err == io.ErrUnexpectedEOF ||
			strings.Contains(err.Error(), "connection reset") ||
			strings.Contains(err.Error(), "broken pipe") ||
			strings.Contains(err.Error(), "use of closed network connection") ||
			strings.Contains(err.Error(), "connection refused") {
			node.removeClient(addr)
			return fmt.Errorf("%w: connection error: %v", ErrNetwork, err)
		}

		if errors.Is(err, ErrApplication) {
			return fmt.Errorf("%w: remote handler returned error: %v", ErrApplication, err)
		}
		node.removeClient(addr)
		return fmt.Errorf("%w: connection error: %v", ErrNetwork, err)
	}

	if method != "MiniNode.Ping" {
		logrus.Infof("[%s] RemoteCall result from %s method %s: reply=%+v", node.Addr, addr, method, reply)
	}
	return nil
}

// Shutdown closes all active client connections in the pool.
func (node *MiniNode) Shutdown() {
	node.poolMutex.Lock()
	defer node.poolMutex.Unlock()

	logrus.Infof("[%s] Shutting down and closing all pooled connections...", node.Addr)
	for addr, client := range node.clientPool {
		if client != nil {
			client.Close()
		}
		delete(node.clientPool, addr)
	}
	logrus.Infof("[%s] All pooled connections closed.", node.Addr)
}

// Ping is a basic RPC method to check if a node is alive.
func (node *MiniNode) Ping(_ string, _ *struct{}) error {
	return nil
}

func (node *MiniNode) Create() {
	logrus.Infof("[%s] Create: To be implemented by a specific protocol.", node.Addr)
}

func (node *MiniNode) Join(addr string) bool {
	logrus.Infof("[%s] Join: To be implemented by a specific protocol (contacting %s).", node.Addr, addr)
	return false
}

func (node *MiniNode) Quit() {
	logrus.Infof("[%s] Quit: Shutting down.", node.Addr)
	node.StopRPCServer()
}

func (node *MiniNode) ForceQuit() {
	logrus.Infof("[%s] ForceQuit: Shutting down immediately.", node.Addr)
	node.StopRPCServer()
}

package naive

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Note: The init() function will be executed when this package is imported.
// See https://golang.org/doc/effective_go.html#init for more details.
func init() {
	// You can use the logrus package to print pretty logs.
	// Here we set the log output to a file.
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

// MiniNode represents a single node in the DHT network.
// This struct contains the basic components required for any node,
// such as its address and RPC server.
// Protocol-specific logic (like data storage and peer management) is omitted.
type MiniNode struct {
	Addr   string // address and port number of the node, e.g., "localhost:1234"
	online bool

	listener net.Listener
	server   *rpc.Server
}

// Init initializes a basic node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *MiniNode) Init(addr string) {
	node.Addr = addr
}

// RunRPCServer starts the RPC server for the node.
// It listens for incoming connections and serves them in separate goroutines.
func (node *MiniNode) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)

	var err error
	node.listener, err = net.Listen("tcp", node.Addr)
	wg.Done() // Signal that the listener is set up.
	if err != nil {
		logrus.Fatalf("[%s] Listen error: %v", node.Addr, err)
	}

	for node.online {
		conn, err := node.listener.Accept()
		if err != nil {
			// Check if the error is due to the listener being closed intentionally.
			if node.online {
				logrus.Errorf("[%s] Accept error: %v", node.Addr, err)
			}
			return
		}
		go node.server.ServeConn(conn)
	}
}

// StopRPCServer gracefully shuts down the node's RPC server.
func (node *MiniNode) StopRPCServer() {
	if !node.online {
		return
	}
	node.online = false
	if node.listener != nil {
		node.listener.Close()
	}
}

// RemoteCall is a helper function to make an RPC call to another node.
// It handles connection dialing, timeout, and closing.
// Re-connecting for every call can be slow. A connection pool could improve performance.
func (node *MiniNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	// Optional: Avoid logging pings to reduce log spam.
	if method != "MiniNode.Ping" {
		logrus.Infof("[%s] RemoteCall -> %s method %s with args %v", node.Addr, addr, method, args)
	}

	// Dial with a timeout to prevent blocking indefinitely on an unresponsive node.
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		logrus.Errorf("[%s] Dialing %s error: %v", node.Addr, addr, err)
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	err = client.Call(method, args, reply)
	if err != nil {
		logrus.Errorf("[%s] RemoteCall to %s method %s error: %v", node.Addr, addr, method, err)
		return err
	}
	return nil
}

//
// RPC Methods
//

// Note: The methods used for RPC must be exported (i.e., Capitalized),
// and must have two arguments, both exported (or builtin) types.
// The second argument must be a pointer.
// The return type must be error.
// In short, the signature of the method must be:
//   func (t *T) MethodName(argType T1, replyType *T2) error
// See https://golang.org/pkg/net/rpc/ for more details.

// Ping is a basic RPC method to check if a node is alive.
// It's useful for health checks and maintaining routing tables.
func (node *MiniNode) Ping(_ string, _ *struct{}) error {
	return nil
}

//
// DHT Interface Methods (to be implemented by a specific protocol)
//

// Run starts the node's services.
func (node *MiniNode) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
}

// Create initializes a new DHT network.
// This is called by the first node in the network.
func (node *MiniNode) Create() {
	// Protocol-specific implementation is required.
	// For example, a Chord node would initialize its finger table and successor.
	logrus.Infof("[%s] Create: To be implemented by a specific protocol.", node.Addr)
}

// Join makes the node join an existing DHT network by contacting a node at `addr`.
func (node *MiniNode) Join(addr string) bool {
	// Protocol-specific implementation is required.
	// For example, a Chord node would use `addr` to find its successor and populate its finger table.
	logrus.Infof("[%s] Join: To be implemented by a specific protocol (contacting %s).", node.Addr, addr)
	return false
}

// Put stores a key-value pair in the DHT.
// The specific protocol will determine which node is responsible for the key.
func (node *MiniNode) Put(key string, value string) bool {
	// Protocol-specific implementation is required.
	logrus.Infof("[%s] Put(%s, %s): To be implemented by a specific protocol.", node.Addr, key, value)
	return false
}

// Get retrieves a value for a given key from the DHT.
// The specific protocol will determine which node is responsible for the key.
func (node *MiniNode) Get(key string) (bool, string) {
	// Protocol-specific implementation is required.
	logrus.Infof("[%s] Get(%s): To be implemented by a specific protocol.", node.Addr, key)
	return false, ""
}

// Delete removes a key-value pair from the DHT.
// The specific protocol will determine which node is responsible for the key.
func (node *MiniNode) Delete(key string) bool {
	// Protocol-specific implementation is required.
	logrus.Infof("[%s] Delete(%s): To be implemented by a specific protocol.", node.Addr, key)
	return false
}

// Quit makes the node gracefully leave the network.
// Protocol-specific logic is needed to transfer data and update other nodes.
func (node *MiniNode) Quit() {
	logrus.Infof("[%s] Quit: Shutting down.", node.Addr)
	// Protocol-specific cleanup (e.g., transferring keys in Chord) should be called here.
	node.StopRPCServer()
}

// ForceQuit makes the node abruptly leave the network.
// No cleanup is performed.
func (node *MiniNode) ForceQuit() {
	logrus.Infof("[%s] ForceQuit: Shutting down immediately.", node.Addr)
	node.StopRPCServer()
}

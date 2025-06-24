package naive

import (
	"net"
	"net/rpc"
	"os"
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
}

func (node *MiniNode) Init(addr string) {
	node.Addr = addr
}

func (node *MiniNode) RunRPCServer(wg *sync.WaitGroup) {
	node.server = rpc.NewServer()
	node.server.Register(node)

	var err error
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
		go node.server.ServeConn(conn)
	}
}

func (node *MiniNode) StopRPCServer() {
	if !node.online {
		return
	}
	node.online = false
	if node.listener != nil {
		node.listener.Close()
	}
}

func (node *MiniNode) RemoteCall(addr string, method string, args interface{}, reply interface{}) error {
	if method != "MiniNode.Ping" {
		logrus.Infof("[%s] RemoteCall -> %s method %s with args %v", node.Addr, addr, method, args)
	}

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

// Ping is a basic RPC method to check if a node is alive.
func (node *MiniNode) Ping(_ string, _ *struct{}) error {
	return nil
}

func (node *MiniNode) Run(wg *sync.WaitGroup) {
	node.online = true
	go node.RunRPCServer(wg)
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

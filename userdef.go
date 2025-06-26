package main

import (
	"dht/chord"
	"dht/kademlia"
	"dht/naive"
)

/*
 * In this file, you need to implement the "NewNode" function.
 * This function should create a new DHT node and return it.
 * You can use the "naive.Node" struct as a reference to implement your own struct.
 */

func NewNode(port int) dhtNode {
	return NewNodeChord(port)
}

func NewNodeKade(port int) dhtNode {
	node := &kademlia.KadeNode{}
	node.MiniNode = &naive.MiniNode{}
	node.Init(portToAddr(localAddress, port))
	return node
}

func NewNodeChord(port int) dhtNode {
	node := &chord.ChordNode{}
	node.MiniNode = &naive.MiniNode{}
	node.Init(portToAddr(localAddress, port))
	return node
}

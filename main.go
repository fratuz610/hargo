package main

import (
	"hargo/discovery"
	"hargo/session"
	"log"
	"net"
	"runtime"
)

//const readCommandList

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	// plumbing
	discovery := discovery.NewDiscovery()
	cache := session.NewCache()
	manager := session.NewManager(discovery, cache)

	// we start a tcp server on port 36379
	ln, err := net.Listen("tcp", ":36379")
	if err != nil {
		log.Fatalf("Unable to listen on port 36379 because: %v", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go func(conn net.Conn) {
			manager.NewCommandSession(conn).Handle()
		}(conn)
	}
}

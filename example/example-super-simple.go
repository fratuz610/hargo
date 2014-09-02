package example

import (
	"log"
	"net"
)

//const readCommandList

func main() {

	// we connect to 127.0.0.1:6379
	redisConn, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatalf("Unable to connect to '127.0.0.1:6379' because %v", err)
	}

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
		go func(clientConn net.Conn) {

			var buf []byte = make([]byte, 4096)
			// we pipe the 2 connections
			var err error
			var read int

			for {
				// we read from the client
				read, err = clientConn.Read(buf)

				if err != nil {
					break
				}

				// we write to the server
				_, err = redisConn.Write(buf[0:read])

				// we read from the server
				read, err = redisConn.Read(buf)

				// we write to the client
				_, err = clientConn.Write(buf[0:read])
			}

			log.Printf("Connection closed: %v\n", err)

		}(conn)
	}
}

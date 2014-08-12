package main

import (
	"fmt"
	//"ha-redis/vendor/github.com/fzzy/radix/redis"
	"log"
	"net"
	//"os"
	//"time"
	"ha-redis/session"
	"strconv"
	"strings"
)

func main() {

	// we start a tcp server on port 36379
	ln, err := net.Listen("tcp", "127.0.0.1:36379")
	if err != nil {
		log.Fatalf("Unable to listen on port 36379 because: %v", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {

	log.Printf("We have an incoming connection!!!\n")

	buffer := make([]byte, 4096)

	for {

		read, err := conn.Read(buffer)

		if err != nil {
			log.Printf("Unable to read from client connection, handleConnection closing down: %v", err)
			return
		}

		log.Printf("Read %d bytes\n", read)

		strList, err := readBulkStringArray(buffer)

		if err != nil {
			log.Printf("Unable to read command because: %v", err)
			return
		}

		log.Printf("We got: '%s'", strings.Join(strList, " / "))

		// we run this session in another goroutine
		go session.NewCommandSession(conn).SendAndReceive(buffer[0:read])
	}

}

func lowReadString(src []byte) ([]byte, string, error) {

	ret := ""
	i := 0
	for {
		if src[i] == 13 && src[i+1] == 10 {
			break
		}
		ret += string(src[i])
		i++
	}

	return src[i+2:], ret, nil
}

func lowReadInteger(src []byte) ([]byte, int, error) {
	src, str, err := lowReadString(src)

	if err != nil {
		return src, -1, err
	}

	number, err := strconv.Atoi(str)
	return src, number, err
}

func readBulkString(src []byte) ([]byte, string, error) {

	// $
	if src[0] != '$' {
		return src, "", fmt.Errorf("Bulk string doesn't start with $ but with '%c' / %d", src[0], src[0])
	}

	src, _, err := lowReadInteger(src[1:])

	if err != nil {
		return src, "", err
	}

	src, str, err := lowReadString(src)

	if err != nil {
		return src, "", err
	}

	return src, str, nil
}

func readBulkStringArray(src []byte) ([]string, error) {

	if src[0] != '*' {
		return nil, fmt.Errorf("Array doesn't start with * but with %c", src[0])
	}

	// we read the number of items
	src, arraySize, err := lowReadInteger(src[1:])

	if err != nil {
		return nil, err
	}

	retList := make([]string, arraySize)

	for i := 0; i < arraySize; i++ {

		src, retList[i], err = readBulkString(src)

		if err != nil {
			return nil, err
		}
	}

	return retList, nil

}

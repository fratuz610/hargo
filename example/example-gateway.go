package main

import (
	"fmt"
	redisServer "ha-redis/vendor/github.com/docker/go-redis-server"
	"ha-redis/vendor/github.com/fzzy/radix/redis"
	"os"
	"time"
)

type MyHandler struct {
	//redisServer.DefaultHandler
	client *redis.Client
}

func (h *MyHandler) errorHandler(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func (h *MyHandler) init() {
	var err error
	h.client, err = redis.DialTimeout("tcp", "127.0.0.1:6379", time.Duration(10)*time.Second)
	h.errorHandler(err)
}

func (h *MyHandler) Get(key string) ([]byte, error) {

	// lazy constructor
	if h.client == nil {
		h.init()
	}

	r := h.client.Cmd("get", key)

	if r.Err != nil {
		return nil, r.Err
	}

	if r.Type == redis.NilReply {
		return nil, nil
	}

	bytes, err := r.Bytes()

	if err != nil {
		return nil, err
	}

	return bytes, nil

}

func (h *MyHandler) Set(key string, value []byte) ([]byte, error) {

	// lazy constructor
	if h.client == nil {
		h.init()
	}

	r := h.client.Cmd("set", key, value)

	if r.Err != nil {
		return nil, r.Err
	}

	bytes, err := r.Bytes()

	if err != nil {
		return nil, err
	}

	fmt.Printf("Set result: %s\n", string(bytes))

	return bytes, nil
}

func (h *MyHandler) Del(key string, keys ...string) (int, error) {

	// lazy constructor
	if h.client == nil {
		h.init()
	}

	r := h.client.Cmd("del", key, keys)

	if r.Err != nil {
		return 0, r.Err
	}

	deleteKeys, err := r.Int()

	if err != nil {
		return 0, err
	}

	fmt.Printf("Del result: %d\n", deleteKeys)

	return deleteKeys, nil
}

func main() {
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("Panic: %v\n", msg)
		}
	}()

	myhandler := &MyHandler{}
	srv, err := redisServer.NewServer(redisServer.DefaultConfig().Port(6380).Handler(myhandler))
	if err != nil {
		panic(err)
	}
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

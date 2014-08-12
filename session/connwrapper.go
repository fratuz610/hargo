package session

import (
	"fmt"
	"log"
	"net"
)

type ConnWrapper struct {
	host      string
	port      int
	redisConn net.Conn
	connected bool
}

func NewConnWrapper(host string, port int) *ConnWrapper {
	return &ConnWrapper{host: host, port: port, connected: false}
}

func (c *ConnWrapper) connect() error {

	var err error

	// if a connection did exist, let's close it
	if c.redisConn != nil {
		c.redisConn.Close()
	}

	c.redisConn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.host, c.port))
	if err != nil {
		return fmt.Errorf("ConnWrapper: Unable to connect to %s:%d because %v", c.host, c.port, err)
	}
	c.connected = true

	log.Printf("ConnWrapper: connected to %s:%d", c.host, c.port)

	return nil
}

func (c *ConnWrapper) Read(b []byte) (n int, err error) {

	if !c.connected {
		err = c.connect()

		if err != nil {
			return 0, err
		}
	}

	n, err = c.redisConn.Read(b)

	if err != nil {
		c.connected = false
	}

	return
}

func (c *ConnWrapper) Write(b []byte) (n int, err error) {

	if !c.connected {
		err = c.connect()

		if err != nil {
			return 0, err
		}
	}

	n, err = c.redisConn.Write(b)

	if err != nil {
		c.connected = false
	}

	return
}

func (c *ConnWrapper) Host() string {
	return c.host
}

func (c *ConnWrapper) Port() int {
	return c.port
}

func (c *ConnWrapper) IsConnected() bool {
	return c.connected
}

func (c *ConnWrapper) Destroy() error {
	if c.redisConn == nil {
		return nil
	}

	return c.redisConn.Close()
}

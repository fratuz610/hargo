package discovery

import (
	"fmt"
	//"log"
	"net"
	"time"
)

type ConnWrapper struct {
	hostPort  string
	redisConn net.Conn
	connected bool
	signature string
}

func NewConnWrapper(hostPort, signature string) *ConnWrapper {
	ret := &ConnWrapper{hostPort: hostPort, connected: false, signature: signature}
	ret.connect()
	return ret
}

func (c *ConnWrapper) connect() error {

	var err error

	// if a connection did exist, let's close it
	if c.redisConn != nil {
		c.redisConn.Close()
	}

	c.redisConn, err = net.Dial("tcp", c.hostPort)
	if err != nil {
		return fmt.Errorf("ConnWrapper: Unable to connect to '%s' because %v", c.hostPort, err)
	}
	c.connected = true

	//log.Printf("ConnWrapper: connected to %s", c.hostPort)

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

func (c *ConnWrapper) SetWriteDeadline(deadline time.Time) error {

	if !c.connected {
		err := c.connect()

		if err != nil {
			return err
		}
	}

	return c.redisConn.SetWriteDeadline(deadline)
}

func (c *ConnWrapper) SetReadDeadline(deadline time.Time) error {

	if !c.connected {
		err := c.connect()

		if err != nil {
			return err
		}
	}

	return c.redisConn.SetReadDeadline(deadline)
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

func (c *ConnWrapper) Signature() string {
	return c.signature
}

func (c *ConnWrapper) HostPort() string {
	return c.hostPort
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

package session

import (
	"fmt"
	"log"
	"net"
)

type ConnPool struct {
	masterConn    *ConnWrapper
	slaveConnList []ConnWrapper
}

func (c *ConnPool) SetMaster(host string, port int) {

	if c.masterConn != nil {
		c.masterConn.Destroy()
	}

	c.masterConn = NewConnWrapper(host, port)
}

func (c *ConnPool) AddSlave(host string, port int) {
	if c.slaveConnList == nil {
		c.slaveConnList = make([]ConnWrapper, 0)
	}

	c.slaveConnList = append(c.slaveConnList, *NewConnWrapper(host, port))
}

func (c *ConnPool) RemoveSlave(host string, port int) {

	newSlaveConnList := make([]ConnWrapper, 0)

	for _, connWrapper := range c.slaveConnList {
		if connWrapper.Host() == host && connWrapper.Port() == port {

			// we destroy the connection
			connWrapper.Destroy()

		} else {
			newSlaveConnList = append(newSlaveConnList, connWrapper)
		}
	}

	c.slaveConnList = newSlaveConnList
}

package session

import (
	"math/rand"
	"sync"
)

type ConnPool struct {
	masterConn    *ConnWrapper
	masterLock    *sync.Mutex
	slaveConnList []*ConnWrapper
	slaveListLock *sync.Mutex
}

func NewConnPool(master *ConnWrapper, slaveList []*ConnWrapper) *ConnPool {
	return &ConnPool{masterConn: master, slaveConnList: slaveList}
}

func (c *ConnPool) GetSlave() *ConnWrapper {

	c.slaveListLock.Lock()
	defer c.slaveListLock.Unlock()

	if len(c.slaveConnList) == 0 {
		return nil
	}

	randIndex := rand.Intn(len(c.slaveConnList))
	ret := c.slaveConnList[randIndex]

	c.slaveConnList = append(c.slaveConnList[:randIndex], c.slaveConnList[randIndex+1:]...)

	return ret
}

func (c *ConnPool) ReturnSlave(conn *ConnWrapper) {

	c.slaveListLock.Lock()
	defer c.slaveListLock.Unlock()

	c.slaveConnList = append(c.slaveConnList, conn)
}

func (c *ConnPool) GetMaster() *ConnWrapper {

	c.masterLock.Lock()
	defer c.masterLock.Unlock()

	ret := c.masterConn

	c.masterConn = nil
	return ret
}

func (c *ConnPool) ReturnMaster(conn *ConnWrapper) {

	c.masterLock.Lock()
	defer c.masterLock.Unlock()

	if c.masterConn == nil {
		c.masterConn = conn
	}
}

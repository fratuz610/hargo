package discovery

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	conPerEndpoint = 50
)

// mutexes

type Discovery struct {
	sentinelsMutex       sync.RWMutex
	sentinelHostPortList []string

	masterHostPort  string
	masterMutex     sync.RWMutex
	masterSignature string
	masterCh        chan *ConnWrapper

	slavesMutex     sync.RWMutex
	slavesSignature string
	slavesCh        chan *ConnWrapper
}

func NewDiscovery() *Discovery {

	d := &Discovery{}

	// we need to start with a master
	var err error
	var masterHost = "127.0.0.1"
	var masterPort = 6379

	if len(os.Args) > 1 {
		masterHost = os.Args[1]
	}

	if len(os.Args) > 2 {
		masterPort, err = strconv.Atoi(os.Args[2])

		if err != nil {
			masterPort = 6379
		}
	}

	// we set the master
	d.masterHostPort = fmt.Sprintf("%s:%d", masterHost, masterPort)
	d.sentinelHostPortList = make([]string, 0)
	d.masterCh = make(chan *ConnWrapper, conPerEndpoint)
	d.slavesCh = make(chan *ConnWrapper, conPerEndpoint*3) // max 3 slaves

	// we setup the master queue only
	d.masterSignature = hash(d.masterHostPort)
	for i := 0; i < conPerEndpoint; i++ {
		d.masterCh <- NewConnWrapper(d.masterHostPort, d.masterSignature)
	}
	// no slaves to start with
	d.slavesSignature = ""

	log.Printf("StartDiscovery: starting with redis master: %s and no slaves\n", d.masterHostPort)

	// first synchronous update
	d.updateSentinels()
	d.updateMasterSlaves()

	// cleanup cache every minute
	go func() {

		timer := time.Tick(time.Duration(30) * time.Second)

		for _ = range timer {
			d.updateSentinels()
			d.updateMasterSlaves()
		}
	}()

	return d
}

func (d *Discovery) GetSlave() *ConnWrapper {

	slavesSignature := d.SlavesSignature()

	for {
		conn := <-d.slavesCh

		if slavesSignature == conn.Signature() {
			return conn
		}
	}
}

func (d *Discovery) ReturnSlave(conn *ConnWrapper) {

	slavesSignature := d.SlavesSignature()

	if conn.Signature() != slavesSignature {
		// we ignore slave connections with an outdated signature
		return
	}

	d.slavesCh <- conn
}

func (d *Discovery) GetMaster() *ConnWrapper {

	masterSignature := d.MasterSignature()

	for {
		conn := <-d.masterCh

		if masterSignature == conn.Signature() {
			return conn
		}
	}
}

func (d *Discovery) ReturnMaster(conn *ConnWrapper) {

	masterSignature := d.MasterSignature()

	if conn.Signature() != masterSignature {
		// we ignore master connections with an outdated signature
		return
	}

	d.masterCh <- conn
}

func (d *Discovery) MasterSignature() string {
	d.masterMutex.RLock()
	defer d.masterMutex.RUnlock()
	return d.masterSignature
}

func (d *Discovery) SlavesSignature() string {
	d.slavesMutex.RLock()
	defer d.slavesMutex.RUnlock()
	return d.slavesSignature
}

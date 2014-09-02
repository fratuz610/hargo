package discovery

import (
	"crypto/sha1"
	"fmt"
	redis "hargo/vendor/github.com/fzzy/radix/redis"
	"io"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"
)

func (d *Discovery) updateSentinels() {

	if d.masterHostPort == "" {
		log.Printf("updateSentinels: ERROR: No master available. This should have not happened!\n")
		return
	}

	log.Printf("updateSentinels: Connecting to master at %s\n", d.masterHostPort)

	// we connect to the master
	master, err := redis.DialTimeout("tcp", d.masterHostPort, time.Duration(5)*time.Second)

	if err != nil {
		log.Fatalf("updateSentinels: Unable to connect to Redis master %s => %v", d.masterHostPort, err)
	}

	// we subscribe to the __sentinel__:hello channel
	log.Printf("updateSentinels: Subscribing to the sentinel hello channel\n")

	r := master.Cmd("subscribe", "__sentinel__:hello")

	if r.Err != nil {
		log.Printf("ERROR: updateSentinels: subscribe call failed %v", r.Err)
		return
	}

	cutOffTime := time.Now().Add(time.Second * time.Duration(4))
	sentinelHostPortList := make([]string, 0)
	sentinelHostPortMap := make(map[string]bool)

	log.Printf("updateSentinels: Sentinel exploration START")

	for {

		if time.Now().After(cutOffTime) {
			log.Printf("updateSentinels: Sentinel exploration END")
			break
		}

		// we listen for sentinels' hellos
		reply := master.ReadReply()

		if len(reply.Elems) != 3 {
			log.Printf("updateSentinels: ERROR reading reply from Sentinel")
			continue
		}

		sentinelStr, _ := reply.Elems[2].Str()
		sentinelList := strings.Split(sentinelStr, ",")

		sentinelHostPort := fmt.Sprintf("%s:%s", sentinelList[0], sentinelList[1])

		if _, ok := sentinelHostPortMap[sentinelHostPort]; ok {
			continue
		}

		log.Printf("updateSentinels: Found sentinel at %s\n", sentinelHostPort)

		sentinelHostPortMap[sentinelHostPort] = true
		sentinelHostPortList = append(sentinelHostPortList, sentinelHostPort)

	}

	log.Printf("Retrieved %d sentinels", len(sentinelHostPortList))

	// we close the connection to the master
	master.Close()

	// we update the reference
	d.sentinelHostPortList = sentinelHostPortList
}

func (d *Discovery) updateMasterSlaves() {

	if d.sentinelHostPortList == nil || len(d.sentinelHostPortList) == 0 {
		log.Printf("updateMasterSlaves: no sentinels available, nothing to do")
		return
	}

	sentinelHostPort := d.sentinelHostPortList[rand.Intn(len(d.sentinelHostPortList))]

	log.Printf("updateMasterSlaves: Connecting to randomly selected sentinel at %s\n", sentinelHostPort)

	sentinel, err := redis.DialTimeout("tcp", sentinelHostPort, time.Duration(5)*time.Second)

	if err != nil {
		log.Printf("ERROR: Unable to connect to sentinel %s => %v", sentinelHostPort, err)
		return
	}

	r := sentinel.Cmd("sentinel", "masters")

	if r.Err != nil {
		log.Printf("ERROR: Sentinel masters call failed %v", r.Err)
		return
	}

	if len(r.Elems) == 0 {
		log.Printf("ERROR: Sentinel reported no masters", r.Err)
		return
	}

	// we pick the first
	r = r.Elems[0]

	masterInfo, err := r.Hash()

	if err != nil {
		log.Printf("ERROR: Malformed Sentinel masters reply", err)
		return
	}

	masterHostPort := fmt.Sprintf("%s:%s", masterInfo["ip"], masterInfo["port"])
	log.Printf("Got master: %s -> name %s", masterHostPort, masterInfo["name"])

	// we update the master reference (if there was a change)

	if d.MasterSignature() != hash(masterHostPort) {

		log.Printf("Master Signature mismatch, updating '%s' to %s'", d.MasterSignature(), hash(masterHostPort))

		d.masterMutex.Lock()
		d.masterSignature = hash(masterHostPort)
		d.masterMutex.Unlock()

		// we enqueue the new connection wrappers
		for i := 0; i < conPerEndpoint; i++ {
			d.masterCh <- NewConnWrapper(masterHostPort, d.masterSignature)
		}
	}

	// we get the slaves
	r = sentinel.Cmd("sentinel", "slaves", masterInfo["name"])

	if len(r.Elems) == 0 {
		log.Printf("ERROR: Sentinel reported no slaves for %s", masterInfo["name"], r.Err)
		return
	}

	slaveHostPortList := make([]string, 0)

	for index, slaveReply := range r.Elems {

		slaveInfo, err := slaveReply.Hash()

		if err != nil {
			log.Printf("ERROR: Malformed Sentinel slaves reply", err)
			return
		}

		slaveHostPort := fmt.Sprintf("%s:%s", slaveInfo["ip"], slaveInfo["port"])

		log.Printf("Slave %s has flags: %s\n", slaveHostPort, slaveInfo["flags"])

		slaveHostPortList = append(slaveHostPortList, slaveHostPort)

		log.Printf("Analyzing slave #%d => %s", index, slaveHostPort)
	}
	// we sort the array (to help with hashing)
	sort.Strings(slaveHostPortList)

	// we update the slave references

	if d.SlavesSignature() != hash(slaveHostPortList...) {

		log.Printf("Slaves Signature mismatch, updating '%s' to '%s'", d.SlavesSignature(), hash(slaveHostPortList...))

		d.slavesMutex.Lock()
		d.slavesSignature = hash(slaveHostPortList...)
		d.slavesMutex.Unlock()

		// we enqueue the new connection wrappers
		for i := 0; i < conPerEndpoint; i++ {
			for _, slaveHostPort := range slaveHostPortList {
				d.slavesCh <- NewConnWrapper(slaveHostPort, d.slavesSignature)
			}
		}
	}

}

func hash(list ...string) string {
	h := sha1.New()
	for _, src := range list {
		io.WriteString(h, src)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

package session

import (
	"fmt"
	redis "hargo/vendor/github.com/fzzy/radix/redis"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

var connPool *ConnPool
var redisConn net.Conn

func init() {

	// we connect to redis sentinel

	var err error
	var sentinelHost = "127.0.0.1"
	var sentinelPort = 26379

	if len(os.Args) > 1 {
		sentinelHost = os.Args[1]
	}

	if len(os.Args) > 2 {
		sentinelPort, err = strconv.Atoi(os.Args[2])

		if err != nil {
			sentinelPort = 26379
		}
	}

	sentinel, err := redis.DialTimeout("tcp", fmt.Sprintf("%s:%d", sentinelHost, sentinelPort), time.Duration(10)*time.Second)

	if err != nil {
		log.Fatalf("Unable to connect to sentinel %s:%d => %v", sentinelHost, sentinelPort, err)
	}

	r := sentinel.Cmd("sentinel", "masters")

	if r.Err != nil {
		log.Fatalf("Sentinel masters call failed %v", r.Err)
	}

	if len(r.Elems) == 0 {
		log.Fatalf("Sentinel reported no masters", r.Err)
	}

	// we pick the first
	r = r.Elems[0]

	masterInfo, err := r.Hash()

	if err != nil {
		log.Fatalf("Malformed Sentinel masters reply", err)
	}

	masterHostPort := fmt.Sprintf("%s:%s", masterInfo["ip"], masterInfo["port"])

	log.Printf("Got master: %s -> name %s", masterHostPort, masterInfo["name"])

	// we connect to the master
	masterConn := NewConnWrapper(masterHostPort)

	// we get the slaves
	r = sentinel.Cmd("sentinel", "slaves", masterInfo["name"])

	if len(r.Elems) == 0 {
		log.Fatalf("Sentinel reported no slaves for %s", masterInfo["name"], r.Err)
	}

	slaveConnList := make([]*ConnWrapper, 0)

	for index, slaveReply := range r.Elems {

		slaveInfo, err := slaveReply.Hash()

		if err != nil {
			log.Fatalf("Malformed Sentinel slaves reply", err)
		}

		slaveHostPort := fmt.Sprintf("%s:%s", slaveInfo["ip"], slaveInfo["port"])

		slaveConnList = append(slaveConnList, NewConnWrapper(slaveHostPort))

		log.Printf("Analyzing slave #%d => %s", index, slaveHostPort)
	}

	// the pool is ready
	connPool = NewConnPool(masterConn, slaveConnList)

	log.Printf("The pool is ready: 1 master and %d slaves\n", len(slaveConnList))
}

func NewCommandSession(client net.Conn) *CommandSession {
	return &CommandSession{client: client, isHA: false}
}

func NewHACommandSession(client net.Conn) *CommandSession {
	return &CommandSession{client: client, isHA: true}
}

type CommandSession struct {
	client net.Conn
	isHA   bool
}

func (c *CommandSession) SendAndReceive(src []byte) {

	var redis *ConnWrapper

	if c.isHA {
		redis = connPool.GetMaster()
	} else {
		redis = connPool.GetSlave()
	}

	defer func(redis *ConnWrapper) {
		if c.isHA {
			connPool.ReturnMaster(redis)
		} else {
			connPool.ReturnSlave(redis)
		}
	}(redis)

	// we write to the redis conn
	writtenSoFar := 0

	for writtenSoFar < len(src) {

		// we time out after 10 seconds
		redis.SetWriteDeadline(time.Now().Add(10 * time.Second))

		written, err := redis.Write(src)

		if err != nil {
			log.Printf("Unable to send commmand to redis because: %v", err)
			c.client.Close()
			return
		}

		writtenSoFar += written
	}

	// we read from redis and write straight back to the client
	readBuf := make([]byte, 4096)

	// we reinit the counter (0 = message complete)
	cnt := 0

	for {

		// we time out after 5 seconds
		redis.SetReadDeadline(time.Now().Add(5 * time.Second))

		read, err := redis.Read(readBuf)

		if err != nil {
			log.Printf("Unable to read response from redis because: %v", err)
			return
		}

		// we fast parse the message to see if it's complete
		cnt = fastParse(readBuf[0:read], cnt)

		// we time out after 5 seconds (client side)
		c.client.SetWriteDeadline(time.Now().Add(5 * time.Second))

		// we write back to the client
		_, err = c.client.Write(readBuf[0:read])

		if err != nil {
			log.Printf("Unable to write response to the client because: %v", err)
			c.client.Close()
			return
		}

		// the message was served complete (no need to read again)
		if cnt == 0 {
			break
		}
	}

}

func fastParse(src []byte, cnt int) int {

	for i := 0; i < len(src); i++ {

		switch src[i] {
		case '*': // array

			length := int(0)

			j := 1
			for j = 1; src[i+j] != '\r'; j++ {
				length = length*10 + int(src[i+j]-'0')
			}

			// we skip the final '\n' + the length of the bulk number string
			i += j + 1

			// we increment the counter for each item in the array
			cnt += length

		case '$': // bulk string

			length := int(0)

			// we read the number of items in the array (which should arrive in one go)
			j := 1
			for j = 1; src[i+j] != '\r'; j++ {
				length = length*10 + int(src[i+j]-'0')
				log.Printf("j: %d / src[i+j]: '%c' length: %d\n", j, src[i+j], length)
			}

			// we skip the final '\n' + the length of the bulk number string
			i += j + 1

			// we skip the bulk string
			i += length

		case '+', '-', ':': // string, error or number

			cnt++
		case '\r':

			if i+1 < len(src) && src[i+1] == '\n' {
				cnt--
				i++
			}
		default:
			// do nothing
		}
	}

	return cnt
}

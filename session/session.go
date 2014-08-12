package session

import (
	"log"
	"net"
	"time"
)

var connPool ConnPool

func initialize() {

	// we connect to redis sentinel

	// we get the list of masters and slaves for "default"

	// we connect to the master and to each of the slaves (ConnWrapper)

	var err error

	redisConn, err = net.Dial("tcp", "10.10.10.10:6381")
	if err != nil {
		log.Fatalf("Unable to connect to redis %v", err)
	}
}

func NewCommandSession(client net.Conn) *CommandSession {

	if redisConn == nil {
		initialize()
	}

	// TODO for now we return one connection only
	return &CommandSession{client: client, redis: redisConn}
}

type CommandSession struct {
	client net.Conn
	redis  net.Conn
}

func (c *CommandSession) SendAndReceive(src []byte) {

	// we write to redis
	writtenSoFar := 0

	for writtenSoFar < len(src) {

		// we time out after 10 seconds
		c.redis.SetWriteDeadline(time.Now().Add(10 * time.Second))

		written, err := c.redis.Write(src)

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
		c.redis.SetReadDeadline(time.Now().Add(5 * time.Second))

		read, err := c.redis.Read(readBuf)

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

			// we read the number of items in the array (which should arrive in one go)
			for j := 0; src[i+j] != '\r'; j++ {
				length = length*10 + int(src[i+j]-'0')
			}

			// we skip the final '\n'
			i++

			// we increment the counter for each item in the array
			cnt += length

		case '$': // bulk string

			length := int(0)

			// we read the number of items in the array (which should arrive in one go)
			for j := 0; src[i+j] != '\r'; j++ {
				length = length*10 + int(src[i+j]-'0')
			}
			// we skip the final '\n'
			i++

			// we skip the bulk string
			i += length

		case '+', '-', ':': // string, error or number
			cnt++
		case '\r':
			if i+1 < len(src) && src[i+1] == '\n' {
				cnt--
			}
		default:
			// do nothing
		}
	}

	return cnt
}

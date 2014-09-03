package session

import (
	"bytes"
	"hargo/discovery"
	"log"
	"net"
	"strings"
	"time"
)

type CommandSession struct {
	manager *Manager
	client  net.Conn
	isHA    bool
	readBuf []byte
}

func (c *CommandSession) Handle() {

	for {

		//log.Printf("readBuf: len %d, cap %d\n", len(readBuf), cap(readBuf))

		read, err := c.client.Read(c.readBuf)

		if err != nil {
			//log.Printf("handleConnection: read error: %v", err)
			break
		}

		//log.Printf("Incoming: '%s'\n", string(c.readBuf[0:read]))

		commandList, err := readRequest(c.readBuf[0:read])

		if err != nil {
			log.Printf("Unable to read command because: %v", err)
			break
		}

		//log.Printf("We got: '%s'", strings.Join(strList, " / "))

		if _, ok := slaveSafeCommandMap[strings.ToLower(commandList[0])]; ok {
			c.isHA = false
		} else {
			c.isHA = true
		}

		// if we have no slaves all requests go to the master
		if c.manager.discov.SlavesSignature() == "" {
			c.isHA = true
		}

		c.sendAndReceive(c.readBuf[0:read])
	}
}

func (c *CommandSession) sendAndReceive(src []byte) {

	var redis *discovery.ConnWrapper
	var command string = string(src) // requests are generally very small

	if bufferedResp := c.manager.cache.Get(command); bufferedResp != nil {

		//log.Printf("Serving cache reply for '%s'\n", command)

		var servedSoFar = 0

		for servedSoFar < len(bufferedResp) {

			// we time out after 5 seconds (client side)
			c.client.SetWriteDeadline(time.Now().Add(5 * time.Second))

			// we write back to the client
			written, err := c.client.Write(bufferedResp[servedSoFar:])

			servedSoFar += written

			if err != nil {
				log.Printf("Unable to write response to the client because: %v", err)
				c.client.Close()
				break
			}
		}

		// we've served a cached reply
		return
	}

	if c.isHA {
		redis = c.manager.discov.GetMaster()
	} else {
		redis = c.manager.discov.GetSlave()
	}

	defer func(redis *discovery.ConnWrapper) {
		if c.isHA {
			c.manager.discov.ReturnMaster(redis)
		} else {
			c.manager.discov.ReturnSlave(redis)
		}
	}(redis)

	// we write to the redis conn
	writtenSoFar := 0

	for writtenSoFar < len(src) {

		// we time out after 10 seconds
		redis.SetWriteDeadline(time.Now().Add(10 * time.Second))

		//log.Printf("Sending '%s'\n", string(src))

		written, err := redis.Write(src)

		if err != nil {
			log.Printf("Unable to send commmand to redis because: %v", err)
			c.client.Close()
			return
		}

		writtenSoFar += written
	}

	//log.Printf("readBuf2: len %d, cap %d\n", len(readBuf), cap(readBuf))

	// we reinit the counter (0 = message complete)
	cnt := 0

	// we allocate a buffer for the reply
	respBuffer := &bytes.Buffer{}
	respBuffer.Grow(4096) // at least 4kb

	for {

		// we time out after 5 seconds
		redis.SetReadDeadline(time.Now().Add(5 * time.Second))

		read, err := redis.Read(c.readBuf)

		if err != nil {
			log.Printf("Unable to read response from redis because: %v", err)
			return
		}

		//log.Printf("Redis reply: '%s'", strings.Trim(string(c.readBuf[0:read]), "\n\r"))

		// we fast parse the message to see if it's complete
		cnt = fastParse(c.readBuf[0:read], cnt)

		// we time out after 5 seconds (client side)
		c.client.SetWriteDeadline(time.Now().Add(5 * time.Second))

		// we save to the buffer
		respBuffer.Write(c.readBuf[0:read])

		// we write back to the client
		_, err = c.client.Write(c.readBuf[0:read])

		if err != nil {
			log.Printf("Unable to write response to the client because: %v", err)
			c.client.Close()
			return
		}

		// the message was served complete (no need to read again)
		if cnt == 0 {
			break
		} else {
			log.Printf("Partial reply: cnt %d => '%s'", cnt, string(c.readBuf[0:read]))
		}
	}

	// we cache the reply if it's not HA
	if !c.isHA {
		c.manager.cache.Put(command, respBuffer.Bytes())
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

			// nil bulk string
			if src[i+1] == '-' && src[i+2] == '1' {
				i += 2
				cnt++
				continue
			}

			length := int(0)

			// we read the number of items in the array (which should arrive in one go)
			j := 1
			for j = 1; src[i+j] != '\r'; j++ {
				length = length*10 + int(src[i+j]-'0')
				//log.Printf("j: %d / src[i+j]: '%c' length: %d\n", j, src[i+j], length)
			}

			// we skip the final '\n' + the length of the bulk number string
			i += j + 1

			// we skip the bulk string
			i += length

			cnt++

		case '-':
			log.Printf("Error returned!")

			cnt++
		case '+', ':': // string, error or number

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

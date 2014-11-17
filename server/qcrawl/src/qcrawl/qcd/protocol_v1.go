package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"qcrawl/qc"
	"qcrawl/util"
	"sync/atomic"
	"time"
)

const maxTimeout = time.Hour

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type ProtocolV1 struct {
	qc.Protocol
}

func init() {
	protocols[string(qc.MagicV1)] = &ProtocolV1{}
}

func (p *ProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	client := NewClientV1(conn)
	for {
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		if *verbose {
			log.Printf("PROTOCOL(V1): [%s] %s", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			context := ""
			if parentErr := err.(qc.ChildError).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

			sendErr := p.Send(client, qc.FrameTypeError, []byte(err.Error()))
			if sendErr != nil {
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*qc.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, qc.FrameTypeResponse, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("PROTOCOL(V2): [%s] exiting ioloop", client)
	// TODO: gracefully send clients the close signal
	conn.Close()
	close(client.ExitChan)

	return err
}

func (p *ProtocolV1) Send(client *ClientV1, frameType int32, data []byte) error {
	client.Lock()
	defer client.Unlock()

	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := qc.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		return err
	}

	if frameType != qc.FrameTypeMessage {
		err = client.Writer.Flush()
	}

	return err
}

func (p *ProtocolV1) Flush(client *ClientV1) error {
	client.Lock()
	defer client.Unlock()

	if client.Writer.Buffered() > 0 {
		client.SetWriteDeadline(time.Now().Add(time.Second))
		return client.Writer.Flush()
	}

	return nil
}

func (p *ProtocolV1) Exec(client *ClientV1, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("IDENTIFY")):
		return p.IDENTIFY(client, params)
	}
	return nil, nil
}

func (p *ProtocolV1) IDENTIFY(client *ClientV1, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != qc.StateInit {
		return nil, qc.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := p.readLen(client)
	if err != nil {
		return nil, qc.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > qcd.options.maxBodySize {
		return nil, qc.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, qcd.options.maxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, qc.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData IdentifyDataV1
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, qc.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	err = client.Identify(identifyData)
	if err != nil {
		return nil, qc.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	resp := okBytes
	if identifyData.FeatureNegotiation {
		resp, err = json.Marshal(struct {
			MaxRdyCount   int64  `json:"max_rdy_count"`
			Version       string `json:"version"`
			MaxMsgTimeout int64  `json:"max_msg_timeout"`
			MsgTimeout    int64  `json:"msg_timeout"`
		}{
			MaxRdyCount:   qcd.options.maxRdyCount,
			Version:       util.BINARY_VERSION,
			MaxMsgTimeout: int64(qcd.options.maxMsgTimeout / time.Millisecond),
			MsgTimeout:    int64(qcd.options.msgTimeout / time.Millisecond),
		})
		if err != nil {
			panic("should never happen")
		}
	}

	return resp, nil
}

func (p *ProtocolV1) readLen(client *ClientV1) (int32, error) {
	client.lenSlice = client.lenSlice[0:]
	_, err := io.ReadFull(client.Reader, client.lenSlice)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(client.lenSlice)), nil
}

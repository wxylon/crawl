package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"qcrawl/qc"
	"sync"
	"sync/atomic"
	"time"
)

type IdentifyDataV1 struct {
	ShortId             string `json:"short_id"`
	LongId              string `json:"long_id"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
}

type ClientV1 struct {
	net.Conn
	sync.Mutex

	// buffered IO
	// 初始化 16k 读缓存
	Reader *bufio.Reader
	// 初始化 16k 写缓存
	Writer *bufio.Writer
	// 5 毫秒
	OutputBufferTimeout           *time.Ticker
	OutputBufferTimeoutUpdateChan chan time.Duration

	State           int32
	ReadyCount      int64
	LastReadyCount  int64
	InFlightCount   int64
	MessageCount    uint64
	FinishCount     uint64
	RequeueCount    uint64
	ConnectTime     time.Time
	ReadyStateChan  chan int
	ExitChan        chan int
	ShortIdentifier string
	LongIdentifier  string

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	lenSlice []byte

	// heartbeats are client configurable via IDENTIFY
	Heartbeat           *time.Ticker
	HeartbeatInterval   time.Duration
	HeartbeatUpdateChan chan time.Duration
}

func NewClientV1(conn net.Conn) *ClientV1 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &ClientV1{
		Conn: conn,

		Reader:                        bufio.NewReaderSize(conn, 16*1024),
		Writer:                        bufio.NewWriterSize(conn, 16*1024),
		OutputBufferTimeout:           time.NewTicker(5 * time.Millisecond),
		OutputBufferTimeoutUpdateChan: make(chan time.Duration, 1),

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan:  make(chan int, 1),
		ExitChan:        make(chan int),
		ConnectTime:     time.Now(),
		ShortIdentifier: identifier,
		LongIdentifier:  identifier,
		State:           qc.StateInit,

		// heartbeats are client configurable but default to 30s
		Heartbeat:           time.NewTicker(qcd.options.clientTimeout / 2),
		HeartbeatInterval:   qcd.options.clientTimeout / 2,
		HeartbeatUpdateChan: make(chan time.Duration, 1),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}

func (c *ClientV1) Identify(data IdentifyDataV1) error {
	c.ShortIdentifier = data.ShortId
	c.LongIdentifier = data.LongId
	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}
	err = c.SetOutputBufferSize(data.OutputBufferSize)
	if err != nil {
		return err
	}
	return c.SetOutputBufferTimeout(data.OutputBufferTimeout)
}

func (c *ClientV1) IsReadyForMessages() bool {

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	lastReadyCount := atomic.LoadInt64(&c.LastReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if *verbose {
		log.Printf("[%s] state rdy: %4d lastrdy: %4d inflt: %4d", c,
			readyCount, lastReadyCount, inFlightCount)
	}

	if inFlightCount >= lastReadyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *ClientV1) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	atomic.StoreInt64(&c.LastReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *ClientV1) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

func (c *ClientV1) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV1) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

func (c *ClientV1) SendingMessage() {
	atomic.AddInt64(&c.ReadyCount, -1)
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *ClientV1) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV1) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *ClientV1) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, qc.StateClosing)
	// TODO: start a timer to actually close the channel (in case the client doesn't do it first)
}

func (c *ClientV1) Pause() {
	c.tryUpdateReadyState()
}

func (c *ClientV1) UnPause() {
	c.tryUpdateReadyState()
}

func (c *ClientV1) SetHeartbeatInterval(desiredInterval int) error {
	// clients can modify the rate of heartbeats (or disable)
	var interval time.Duration

	switch {
	case desiredInterval == -1:
		interval = -1
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(qcd.options.maxHeartbeatInterval/time.Millisecond):
		interval = (time.Duration(desiredInterval) * time.Millisecond)
	default:
		return errors.New(fmt.Sprintf("heartbeat interval (%d) is invalid", desiredInterval))
	}

	// leave the default heartbeat in place
	if desiredInterval != 0 {
		select {
		case c.HeartbeatUpdateChan <- interval:
		default:
		}
		c.HeartbeatInterval = interval
	}

	return nil
}

func (c *ClientV1) SetOutputBufferSize(desiredSize int) error {
	c.Lock()
	defer c.Unlock()

	var size int

	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		size = 1
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(qcd.options.maxOutputBufferSize):
		size = desiredSize
	default:
		return errors.New(fmt.Sprintf("output buffer size (%d) is invalid", desiredSize))
	}

	if size > 0 {
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, size)
	}

	return nil
}

func (c *ClientV1) SetOutputBufferTimeout(desiredTimeout int) error {
	var timeout time.Duration

	switch {
	case desiredTimeout == -1:
		timeout = -1
	case desiredTimeout == 0:
		// do nothing (use default)
	case desiredTimeout >= 5 &&
		desiredTimeout <= int(qcd.options.maxOutputBufferTimeout/time.Millisecond):
		timeout = (time.Duration(desiredTimeout) * time.Millisecond)
	default:
		return errors.New(fmt.Sprintf("output buffer timeout (%d) is invalid", desiredTimeout))
	}

	if desiredTimeout != 0 {
		select {
		case c.OutputBufferTimeoutUpdateChan <- timeout:
		default:
		}
	}

	return nil
}

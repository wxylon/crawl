package main

import (
	"log"
	"net"
	"os"
	"qcrawl/qc"
	"qcrawl/util"
	"sync"
	"time"
)

type Notifier interface {
	Notify(v interface{})
}

type QCd struct {
	sync.RWMutex
	// 配置信息
	options *qcOptions
	// 监听的tcp端口:0.0.0.0:4150
	tcpAddr *net.TCPAddr
	// 监听的http端口:0.0.0.0:4151
	httpAddr *net.TCPAddr

	topicMap map[string]string

	// tcp 请求处理的 监听器
	tcpListener net.Listener
	// http 请求处理的 监听器
	httpListener net.Listener
	// 容器退出标识,
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
	notifyChan chan interface{}
}

type qcOptions struct {
	memQueueSize         int64
	dataPath             string
	maxMessageSize       int64
	maxBodySize          int64
	maxBytesPerFile      int64
	maxRdyCount          int64
	syncEvery            int64
	syncTimeout          time.Duration
	msgTimeout           time.Duration
	maxMsgTimeout        time.Duration
	clientTimeout        time.Duration
	maxHeartbeatInterval time.Duration
	broadcastAddress     string

	maxOutputBufferSize    int64
	maxOutputBufferTimeout time.Duration
}

/** 创建 NSQd 配置信息 */
func NewQcdOptions() *qcOptions {
	return &qcOptions{
		memQueueSize:         10000,
		dataPath:             os.TempDir(),
		maxMessageSize:       1024768,
		maxBodySize:          5 * 1024768,
		maxBytesPerFile:      104857600,
		maxRdyCount:          2500,
		syncEvery:            2500,
		syncTimeout:          2 * time.Second,
		msgTimeout:           60 * time.Second,
		maxMsgTimeout:        15 * time.Minute,
		clientTimeout:        qc.DefaultClientTimeout,
		maxHeartbeatInterval: 60 * time.Second,
		broadcastAddress:     "",

		maxOutputBufferSize:    64 * 1024,
		maxOutputBufferTimeout: 1 * time.Second,
	}
}

func NewQCd(options *qcOptions) *QCd {
	q := &QCd{
		options:    options,
		topicMap:   make(map[string]string),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
	}

	return q
}

func (n *QCd) Main() {

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	}
	n.tcpListener = tcpListener
	n.waitGroup.Wrap(func() { util.TcpServer(n.tcpListener, &TcpProtocol{protocols: protocols}) })

	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	n.waitGroup.Wrap(func() { httpServer(n.httpListener) })
}

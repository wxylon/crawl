package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"qcrawl/qc"
	"qcrawl/util"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

var (
	verbose     = flag.Bool("verbose", false, "enable verbose logging")
	showVersion = flag.Bool("version", false, "print version string")

	httpAddress = flag.String("http-address", "0.0.0.0:3001", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress  = flag.String("tcp-address", "0.0.0.0:3002", "<addr>:<port> to listen on for TCP clients")

	msgTimeout     = flag.String("msg-timeout", "60s", "duration to wait before auto-requeing a message")
	maxMsgTimeout  = flag.Duration("max-msg-timeout", 15*time.Minute, "maximum duration before a message will timeout")
	maxMessageSize = flag.Int64("max-message-size", 1024768, "maximum size of a single message in bytes")
	maxBodySize    = flag.Int64("max-body-size", 5*1024768, "maximum size of a single command body")

	maxHeartbeatInterval   = flag.Duration("max-heartbeat-interval", 60*time.Second, "maximum client configurable duration of time between client heartbeats")
	maxRdyCount            = flag.Int64("max-rdy-count", 2500, "maximum RDY count for a client")
	maxOutputBufferSize    = flag.Int64("max-output-buffer-size", 64*1024, "maximum client configurable size (in bytes) for a client output buffer")
	maxOutputBufferTimeout = flag.Duration("max-output-buffer-timeout", 1*time.Second, "maximum client configurable duration of time between flushing to a client")
)

var qcd *QCd

var protocols = map[string]qc.Protocol{}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println(util.Version("nsqd"))
		return
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	msgTimeoutDuration := flagToDuration(*msgTimeout, time.Millisecond, "--msg-timeout")

	options := NewQcdOptions()
	options.maxRdyCount = *maxRdyCount
	options.maxMessageSize = *maxMessageSize

	options.maxBodySize = *maxBodySize

	options.msgTimeout = msgTimeoutDuration
	options.maxMsgTimeout = *maxMsgTimeout
	options.maxHeartbeatInterval = *maxHeartbeatInterval
	options.maxOutputBufferSize = *maxOutputBufferSize
	options.maxOutputBufferTimeout = *maxOutputBufferTimeout

	qcd = NewQCd(options)
	qcd.tcpAddr = tcpAddr
	qcd.httpAddr = httpAddr

	qcd.Main()

	<-exitChan
}

func flagToDuration(val string, mult time.Duration, flag string) time.Duration {
	if regexp.MustCompile(`^[0-9]+$`).MatchString(val) {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			log.Fatalf("ERROR: failed to Atoi %s=%s - %s", flag, val, err.Error())
		}
		return time.Duration(intVal) * mult
	}

	duration, err := time.ParseDuration(val)
	if err != nil {
		log.Fatalf("ERROR: failed to ParseDuration %s=%s - %s", flag, val, err.Error())
	}
	return duration
}

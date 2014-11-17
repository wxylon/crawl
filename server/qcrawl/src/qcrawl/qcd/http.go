package main

import (
	"io"
	"log"
	"net"
	"net/http"
	httpprof "net/http/pprof"
	"os"
	"qcrawl/util"
	"runtime/pprof"
	"strings"
)

func httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/info", infoHandler)
	handler.HandleFunc("/mem_profile", memProfileHandler)
	handler.HandleFunc("/cpu_profile", httpprof.Profile)

	// these timeouts are absolute per server connection NOT per request
	// this means that a single persistent connection will only last N seconds
	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}

	log.Printf("HTTP: closing %s", listener.Addr().String())
}

func memProfileHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("MEMORY Profiling Enabled")
	f, err := os.Create("nsqd.mprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(f)
	f.Close()

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, "OK", struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

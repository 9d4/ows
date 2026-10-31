package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "serve":
		execServe(args)
	case "connect":
		execConnect(args)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
	}
}

func printUsage() {
	fmt.Print("Usage: ows <command> [options]\n\n")
	fmt.Print("Commands:\n")
	fmt.Print("  serve         Start serving through tunnel\n")
	fmt.Print("  connect       Connect to tunnel\n")
}

func printServeUsage() {
	fmt.Print("Usage: ows serve <from> [to]\n\n")
	fmt.Print("Args:\n")
	fmt.Print("  from          Address to listen from (eg. 127.0.0.1:3306)\n")
	fmt.Print("  to            HTTP Address to listen to (eg. 127.0.0.1:33066)\n")
}

func execServe(args []string) {
	if len(args) < 1 {
		printServeUsage()
	}

	from, err := net.ResolveTCPAddr("tcp6", args[0])
	if err != nil {
		log.Fatalf("parse from: %v", err)
	}

	to := &net.TCPAddr{IP: net.IPv6zero}
	if len(args) > 1 {
		to, err = net.ResolveTCPAddr("tcp6", args[1])
	}

	// err = serveListen(from, to)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	err = serveListenWebSocket(from, to)
	if err != nil {
		log.Fatal(err)
	}
}

func execConnect(args []string) {
	panic("unimplemented")
}

func serveListen(from *net.TCPAddr, to *net.TCPAddr) error {
	listener, err := net.ListenTCP("tcp6", to)
	if err != nil {
		return err
	}

	log.Printf("Listening on %s", listener.Addr())
	log.Printf("Forwarding to %s", from)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error accepting connection: %s", err)
			continue
		}

		go serveHandleConn(conn, from)
	}
}

func serveHandleConn(conn net.Conn, peerAddress *net.TCPAddr) {
	l := slog.With(slog.String("remote", conn.RemoteAddr().String()))
	l.Info("handling connection")
	l.Debug("connecting to peer")

	peer, err := net.DialTCP("tcp6", nil, peerAddress)
	if err != nil {
		l.Error("cannot connect to peer", "err", err)
		l.Info("closing connection")
		conn.Close()
		return
	}

	l.Debug("connected to peer")

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()

		_, err := io.Copy(conn, peer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				l.Info("conn closed")
				return
			}
			l.Error("copying peer write to conn", "err", err)
		}
	}()

	go func() {
		defer cancel()

		_, err := io.Copy(peer, conn)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				l.Info("peer closed")
				return
			}
			l.Error("copying conn write to peer", "err", err)
		}
	}()

	<-ctx.Done()
	l.Info("closing both connections")
	peer.Close()
	conn.Close()
}

func serveListenWebSocket(from *net.TCPAddr, to *net.TCPAddr) error {
	handler := &ServeHandler{
		forwardAddress: from,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
	srv := http.Server{
		Handler: handler,
	}

	ln, err := net.ListenTCP("tcp", to)
	if err != nil {
		return err
	}

	_ = from

	log.Printf("Listening on %s", ln.Addr())
	if err := srv.Serve(ln); err != nil {
		return err
	}

	return nil
}

type ServeHandler struct {
	forwardAddress *net.TCPAddr
	upgrader       websocket.Upgrader
}

func (h *ServeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l := slog.With(slog.String("remote", r.RemoteAddr))
	l.Info("handling connection")
	l.Debug("connecting to peer")

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("cannot upgrade connection", "err", err, "remotews", r.RemoteAddr)
		return
	}

	peer, err := net.DialTCP("tcp6", nil, h.forwardAddress)
	if err != nil {
		slog.Error("cannot connect to peer", "err", err, "remotews", r.RemoteAddr)
		conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "cannot connect to peer"),
			time.Now().Add(30*time.Second),
		)
		conn.Close()
		return
	}

	l.Debug("connected to peer")
	ctx, cancel := context.WithCancel(context.Background())

	writer := func(p []byte) (n int, err error) {
		n = len(p)
		err = conn.WriteMessage(websocket.TextMessage, p)
		l.Debug("writing to websocket", "len", n, "err", err)
		l.Debug(fmt.Sprintf("peer -> conn: %d bytes", n))
		l.Debug("===================================")
		for line := range strings.SplitSeq(string(p), "\r") {
			l.Debug(line)
		}
		l.Debug("===================================")
		return
	}

	msg := `GET / HTTP/1.1
Host: localhost:5173
User-Agent: curl/8.20.0
Accept: */*
Transfer-Encoding: chunked0\r\n\r\n
`

	go func() {
		time.Sleep(100 * time.Millisecond)
		l.Debug("writing HTTP to peer", "msg", msg)

		if _, err := peer.Write([]byte(msg)); err != nil {
			l.Error("writing to peer", "err", err)
		}
		_ = writer
	}()

	go func() {
		defer cancel()

		_, err := io.Copy(WriterFunc(writer), peer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				l.Info("conn closed")
				return
			}
			l.Error("copying peer write to conn", "err", err)
		}
	}()

	// go func() {
	// 	defer cancel()
	//
	// 	_, err := io.Copy(peer, conn.NetConn())
	// 	if err != nil {
	// 		if errors.Is(err, net.ErrClosed) {
	// 			l.Info("peer closed")
	// 			return
	// 		}
	// 		l.Error("copying conn write to peer", "err", err)
	// 	}
	// }()

	<-ctx.Done()
	l.Info("closing both connections")
	peer.Close()
	conn.Close()
}

type WriterFunc func(p []byte) (n int, err error)

func (f WriterFunc) Write(p []byte) (n int, err error) {
	return f(p)
}

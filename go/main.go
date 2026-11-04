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
	"net/url"
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

func printConnectUsage() {
	fmt.Print("Usage: ows connect <from> [to]\n\n")
	fmt.Print("Args:\n")
	fmt.Print("  from          Websocket server address (eg. ws://localhost:33066)\n")
	fmt.Print("  to            Address to listen forwarded connection (eg. 127.0.0.1:3306)\n")
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
	if len(args) < 1 {
		printServeUsage()
		return
	}

	knownSchemes := []string{"ws", "wss", "http", "https"}
	hasScheme := false
	for _, s := range knownSchemes {
		hasScheme = strings.HasPrefix(args[0], s+"://")
		if hasScheme {
			continue
		}
	}

	// Set default scheme to ws://
	if !hasScheme {
		args[0] = "ws://" + args[0]
	}

	from, err := url.Parse(args[0])
	if err != nil {
		log.Fatalf("parse from: %v", err)
	}

	to := &net.TCPAddr{IP: net.IPv6zero}
	if len(args) > 1 {
		to, err = net.ResolveTCPAddr("tcp6", args[1])
		if err != nil {
			log.Fatalf("parse to: %v", err)
		}
	}

	err = connectListen(from, to)
	if err != nil {
		log.Fatal(err)
	}
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

	wsReader := ReaderFunc(func(p []byte) (n int, err error) {
		t, msg, err := conn.ReadMessage()
		if err != nil {
			l.Error("error reading from websocket", "err", err)
			cancel()
			return 0, err
		}

		l.Debug("received message from websocket", "type", t, "len", len(msg))
		return copy(p, msg), nil
	})
	wsWriter := WriterFunc(func(p []byte) (n int, err error) {
		err = conn.WriteMessage(websocket.BinaryMessage, p)
		if err != nil {
			l.Error("error writing to websocket", "err", err)
			cancel()
			return 0, err
		}

		l.Debug("wrote message to websocket", "len", len(p))
		return len(p), nil
	})

	go func() {
		defer cancel()

		_, err := io.Copy(wsWriter, peer)
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

		_, err := io.Copy(peer, wsReader)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				l.Info("peer closed")
				return
			}
			l.Error("copying conn write to peer", "err", err)
		}
	}()

	<-ctx.Done()
	peer.Close()
	conn.Close()
	l.Info("connections closed")
}

type WriterFunc func(p []byte) (n int, err error)

func (f WriterFunc) Write(p []byte) (n int, err error) {
	return f(p)
}

type ReaderFunc func(p []byte) (n int, err error)

func (f ReaderFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

func connectListen(uri *url.URL, to *net.TCPAddr) error {
	lis, err := net.ListenTCP("tcp", to)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", to, err)
	}

	log.Printf("Listening on %s", lis.Addr())
	log.Printf("Forwarding to %s", uri.String())

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("error accepting connection: %s", err)
			continue
		}

		go connectHandleConn(conn, uri)
	}
}

func connectHandleConn(conn net.Conn, uri *url.URL) {
	l := slog.With(
		slog.String("remote", conn.RemoteAddr().String()),
		slog.String("uri", uri.String()),
	)
	l.Info("handling connection")

	ws, _, err := websocket.DefaultDialer.Dial(uri.String(), nil)
	if err != nil {
		l.Error("cannot connect to websocket server", "err", err)
		l.Info("closing connection")
		conn.Close()
		return
	}

	l.Debug("connected to websocket server")

	ctx, cancel := context.WithCancel(context.Background())

	wsReader := ReaderFunc(func(p []byte) (n int, err error) {
		t, msg, err := ws.ReadMessage()
		if err != nil {
			l.Error("error reading from websocket", "err", err)
			cancel()
			return 0, err
		}

		l.Debug("received message from websocket", "type", t, "len", len(msg))
		return copy(p, msg), nil
	})
	wsWriter := WriterFunc(func(p []byte) (n int, err error) {
		err = ws.WriteMessage(websocket.BinaryMessage, p)
		if err != nil {
			l.Error("error writing to websocket", "err", err)
			cancel()
			return 0, err
		}

		l.Debug("wrote message to websocket", "len", len(p))
		return len(p), nil
	})

	go func() {
		defer cancel()

		_, err = io.Copy(conn, wsReader)
		if err != nil {
			l.Error("copying websocket -> conn", "err", err)
		}
	}()

	go func() {
		defer cancel()

		_, err = io.Copy(wsWriter, conn)
		if err != nil {
			l.Error("copying conn -> websocket", "err", err)
		}
	}()

	<-ctx.Done()
	ws.Close()
	conn.Close()

	l.Info("connections closed")
}

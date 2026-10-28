package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
)

func main() {
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

	err = serveListen(from, to)
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

/*
MIT License

Copyright (c) 2024-2026 The Trzsz SSH Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"

	"charm.land/wish/v2"
	"github.com/charmbracelet/ssh"
	"github.com/trzsz/tsshd/tsshd"
	"golang.org/x/term"
)

const (
	host = "localhost"
	port = "56789"
	key  = ".ssh/id_ed25519"
)

// These type aliases define a *common session abstraction*.
//
// tsshd and wish use different session interfaces, but we unify them
// into a single interface type (tsshd.Session) so business logic
// (like handleSession) can be reused across both servers.
type Session = tsshd.Session
type PtyState = tsshd.PtyState
type Window = tsshd.Window

// writeCloserAdapter adapts an io.ReadWriter into an io.WriteCloser.
//
// This is required because tsshd expect Stderr() to return an io.WriteCloser,
// while wish only provides an io.ReadWriter for stderr.
type writeCloserAdapter struct {
	rw io.ReadWriter
}

func (w *writeCloserAdapter) Write(p []byte) (int, error) {
	return w.rw.Write(p)
}

func (w *writeCloserAdapter) Close() error {
	return nil
}

// wishSessionAdapter adapts a wish SSH session into a tsshd-compatible Session.
//
// This is the key design point:
// we wrap wish.Session and implement the same interface expected by tsshd,
// allowing both server implementations to share the same handler logic.
type wishSessionAdapter struct {
	ssh.Session

	// used to ensure PTY window channel is only converted once
	chOnce sync.Once

	// normalized window resize channel
	winCh <-chan Window
}

// Stderr adapts wish stderr stream into tsshd-compatible io.WriteCloser.
func (s *wishSessionAdapter) Stderr() io.WriteCloser {
	return &writeCloserAdapter{rw: s.Session.Stderr()}
}

// Pty normalizes PTY state and window events across wish and tsshd.
//
// Both systems support PTY, but their APIs differ:
// - wish provides ssh.Pty() with ssh.Window
// - tsshd expects tsshd.PtyState + <-chan tsshd.Window
//
// This adapter ensures both are unified into a single format.
func (s *wishSessionAdapter) Pty() (PtyState, <-chan Window, bool) {
	state, winCh, pty := s.Session.Pty()

	if !pty {
		return PtyState{}, nil, false
	}

	// Convert wish window events into tsshd-compatible Window events.
	s.chOnce.Do(func() {
		out := make(chan Window, 1)

		go func() {
			defer close(out)

			for w := range winCh {
				out <- Window{
					Width:  w.Width,
					Height: w.Height,
				}
			}
		}()

		s.winCh = out
	})

	return PtyState{
		Term: state.Term,
		Window: Window{
			Width:  state.Window.Width,
			Height: state.Window.Height,
		}}, s.winCh, pty
}

// Context adapts wish session's context into tsshd-compatible context.Context.
func (s *wishSessionAdapter) Context() context.Context {
	return s.Session.Context()
}

func main() {
	// ------------------------------------------------------------
	// MODE 1: Run as tsshd server
	// ------------------------------------------------------------
	//
	// This allows the same binary to act as a tsshd-compatible SSH server.
	if len(os.Args) > 1 && os.Args[1] == "tsshd" {
		code, err := tsshd.RunMain(tsshd.WithMiddleware(
			func(next tsshd.Handler) tsshd.Handler {
				return func(sess tsshd.Session) {
					// Try custom handler first; fallback to default pipeline
					if !handleBusiness(sess) {
						next(sess)
					}
				}
			},
		))
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
		os.Exit(code)
		return
	}

	// ------------------------------------------------------------
	// MODE 2: Run as wish SSH server
	// ------------------------------------------------------------
	//
	// We adapt it into the same Session interface using wishSessionAdapter.
	addr := net.JoinHostPort(host, port)
	server, err := wish.NewServer(
		wish.WithAddress(addr),
		wish.WithHostKeyPath(key),
		wish.WithMiddleware(
			// Middleware #1:
			// Convert wish session into tsshd-compatible session,
			// then reuse shared handleSession logic.
			func(next ssh.Handler) ssh.Handler {
				return func(sess ssh.Session) {
					if !handleBusiness(&wishSessionAdapter{Session: sess}) {
						next(sess)
					}
				}
			},
			// Middleware #2:
			// Optional command-based routing.
			// If the client explicitly requests "tsshd",
			// we can switch execution mode dynamically.
			func(next ssh.Handler) ssh.Handler {
				return func(sess ssh.Session) {
					if cmds := sess.Command(); len(cmds) > 0 && cmds[0] == "tsshd" {
						launchTsshServer(cmds, sess)
						return
					}
					next(sess)
				}
			},
		),
	)
	if err != nil {
		log.Fatalf("new wish server failed: %v", err)
	}

	log.Printf("Starting with server on %s", addr)
	if err = server.ListenAndServe(); err != nil && !errors.Is(err, ssh.ErrServerClosed) {
		log.Fatalf("start wish server failed: %v", err)
	}
}

// handleBusiness contains the *shared business logic* for both SSH servers.
//
// This function is the core of the design:
// it works identically for both wishSessionAdapter and tsshd.Session.
//
// This is the main benefit of the adapter pattern used above.
func handleBusiness(sess Session) bool {
	_, _ = fmt.Fprintf(sess, "Hello! My type is %T.\r\n", sess)

	term := term.NewTerminal(sess, "What's your name? ")
	name, err := term.ReadLine()
	if err != nil {
		_, _ = fmt.Fprintf(sess, "term.ReadLine error: %v", err)
		return true
	}

	_, _ = fmt.Fprintf(sess, "Nice to meet you, %s. Goodbye!\r\n", name)
	return true
}

// launchTsshServer hands off the current SSH session to a tsshd-backed
// execution mode by re-executing the current binary as a subprocess.
//
// The subprocess inherits the parent environment, except that
// SSH_CONNECTION is rebuilt from the active session so the new process
// sees the correct client/server endpoints.
//
// Stdout and stderr are merged and streamed to the session. The first
// line of output is treated as a readiness signal: once observed, this
// function exits the current session, allowing the subprocess to take
// over the session lifecycle.
//
// Internally, the subprocess acts as a short-lived launcher: it is
// expected to exec/spawn the long-lived tsshd process and then exit.
// We only wait for and reap this intermediate process; the actual
// service continues running independently.
//
// This pattern enables a hybrid SSH server that can seamlessly hand off
// an existing session to tsshd (e.g. for tssh roaming) within the same
// application (the same binary).
func launchTsshServer(args []string, sess ssh.Session) {
	// Resolve the current executable path for re-exec.
	exePath, err := os.Executable()
	if err != nil {
		_, _ = fmt.Fprintf(sess, "failed to get current executable path: %v\r\n", err)
		return
	}

	// Spawn a subprocess running the same binary with provided args.
	cmd := exec.Command(exePath, args...)

	// Merge stdout and stderr into a single stream.
	reader, writer := io.Pipe()
	cmd.Stdout = writer
	cmd.Stderr = writer

	// Rebuild environment variables, overriding SSH_CONNECTION with
	// the current session's addresses.
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "SSH_CONNECTION=") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, fmt.Sprintf("SSH_CONNECTION=%s %s",
		addrToString(sess.RemoteAddr()), addrToString(sess.LocalAddr())))

	// Start the subprocess.
	if err := cmd.Start(); err != nil {
		_, _ = fmt.Fprintf(sess, "failed to start subprocess: %v\r\n", err)
		return
	}

	// Wait until the first line of output is produced, which is treated
	// as a readiness signal from the subprocess.
	scanner := bufio.NewScanner(reader)
	if scanner.Scan() {
		_, _ = fmt.Fprintf(sess, "%s\n", scanner.Text())
	}

	// Close pipes to release resources and unblock writers.
	_ = writer.Close()
	_ = reader.Close()

	// Exit the current session; the subprocess is expected to take over.
	_ = sess.Exit(0)

	// Reap the intermediate subprocess.
	_ = cmd.Wait()
}

// Format net.Addr into "host port" form compatible with SSH_CONNECTION.
func addrToString(addr net.Addr) string {
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return fmt.Sprintf("%s %s", host, port)
}

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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"

	"github.com/creack/pty"
	"github.com/trzsz/tsshd/tsshd"
)

// main starts a minimal tsshd-based SSH application.
//
// This binary is NOT a standalone SSH server in the traditional sense.
// Instead, it is executed remotely by an OpenSSH server via tssh.
//
// The tssh client connects to a normal OpenSSH server, then launches
// this binary on the remote machine and connects to it.
//
// If your application binary is not named `tsshd` or is not available in PATH,
// you must explicitly specify its absolute path using either:
//  1. the `--tsshd-path` CLI option, or
//  2. the `TsshdPath` field in your `~/.ssh/config`.
func main() {
	// tsshd.RunMain is the entry point that handles standard tsshd flags and logic.
	// We use tsshd.WithMiddleware to inject our custom logic into the SSH session.
	code, err := tsshd.RunMain(
		tsshd.WithMiddleware(sshdMiddleware),
	)

	// Handle global execution errors (e.g., flag parsing failure).
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	// Exit with the code returned by RunMain.
	os.Exit(code)
}

// sshdMiddleware is a middleware that wraps the session handler.
//
// Middleware allows you to intercept or extend session behavior.
// Here we completely handle the session without calling next.
func sshdMiddleware(next tsshd.Handler) tsshd.Handler {
	return func(sess tsshd.Session) {
		_, _ = fmt.Fprint(sess, "--- Welcome to tsshd (Custom SSH Application) ---\r\n")

		// Prepare the execution command
		var cmd *exec.Cmd
		args := sess.Command()
		if len(args) == 0 {
			// No command provided, use the default shell
			shell := os.Getenv("SHELL")
			if shell == "" {
				if runtime.GOOS == "windows" {
					shell = "powershell"
				} else {
					shell = "/bin/sh"
				}
			}
			cmd = exec.Command(shell)
		} else {
			// Command provided by the client
			cmd = exec.Command(args[0], args[1:]...)
		}

		// Apply environment variables provided by the SSH session
		cmd.Env = sess.Environ()

		// Handle PTY (Pseudo-Terminal) vs Direct Execution
		ptyState, winCh, isPty := sess.Pty()

		if isPty { // --- PTY FLOW (Interactive Mode) ---

			// Start the command using the pty library with the initial window size
			ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
				Cols: uint16(ptyState.Window.Width),
				Rows: uint16(ptyState.Window.Height),
			})
			if err != nil {
				_, _ = fmt.Fprintf(sess.Stderr(), "start pty failed: %v\n", err)
				_ = sess.Exit(201)
				return
			}
			defer func() { _ = ptmx.Close() }()

			// Handle window resize events
			go func() {
				for win := range winCh {
					_ = pty.Setsize(ptmx, &pty.Winsize{
						Cols: uint16(win.Width),
						Rows: uint16(win.Height),
					})
				}
			}()

			// Bidirectional PTY I/O forwarding
			go func() { _, _ = io.Copy(ptmx, sess) }()
			go func() { _, _ = io.Copy(sess, ptmx) }()

			// Close stderr early as it is unused in PTY mode
			_ = sess.Stderr().Close()

			// Ensure the PTY is closed if the client quits
			defer context.AfterFunc(sess.Context(), func() { _ = ptmx.Close() })()

		} else { // --- DIRECT FLOW (Batch/Exec Mode) ---

			// Create a pipe for stdin to prevent cmd.Wait() from blocking on the session's Read()
			stdinPipe, err := cmd.StdinPipe()
			if err != nil {
				_, _ = fmt.Fprintf(sess.Stderr(), "stdin pipe failed: %v\n", err)
				_ = sess.Exit(202)
				return
			}

			// Map output streams directly to the SSH session
			cmd.Stdout = sess
			cmd.Stderr = sess.Stderr()

			// Start the command execution without waiting for it to finish
			if err := cmd.Start(); err != nil {
				_, _ = fmt.Fprintf(sess.Stderr(), "start cmd failed: %v\n", err)
				_ = sess.Exit(203)
				return
			}

			// Forward stdin in a background goroutine so it doesn't block cmd.Wait()
			go func() {
				_, _ = io.Copy(stdinPipe, sess)
				_ = stdinPipe.Close()
			}()

			// Ensure the process is terminated if the client quits
			defer context.AfterFunc(sess.Context(), func() { _ = cmd.Process.Kill() })()
		}

		// Wait for process completion and propagate exit code
		if err := cmd.Wait(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				_ = sess.Exit(exitErr.ExitCode())
				return
			}
			_ = sess.Exit(204)
			return
		}
	}
}

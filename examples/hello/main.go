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
	"fmt"
	"os"

	"github.com/trzsz/tsshd/tsshd"
	"golang.org/x/term"
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
		tsshd.WithMiddleware(helloMiddleware),
	)

	// Handle global execution errors (e.g., flag parsing failure).
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	// Exit with the code returned by RunMain.
	os.Exit(code)
}

// helloMiddleware is a middleware that wraps the session handler.
//
// Middleware allows you to intercept or extend session behavior.
// Here we completely handle the session without calling next.
func helloMiddleware(next tsshd.Handler) tsshd.Handler {
	return func(sess tsshd.Session) {
		// Print a greeting.
		_, _ = fmt.Fprint(sess, "Hello!\r\n")

		// Read a single line of input from the user.
		term := term.NewTerminal(sess, "What's your name? ")
		name, err := term.ReadLine()
		if err != nil {
			_, _ = fmt.Fprintf(sess, "ReadLine error: %v\r\n", err)
			return
		}

		// Respond to the user.
		_, _ = fmt.Fprintf(sess, "Nice to meet you, %s. Goodbye!\r\n", name)
	}
}

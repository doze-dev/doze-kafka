// Command apisurface prints the exported surface of a Go package, one
// declaration per line, with no documentation and no bodies: the thing a
// compatibility promise is about. `task api` compares it with a committed copy.
//
//	go run ./scripts/apisurface ./server
package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: apisurface <package directory>")
		os.Exit(2)
	}
	lines, err := Surface(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(strings.Join(lines, "\n"))
}

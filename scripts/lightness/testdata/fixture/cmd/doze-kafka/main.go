// Command doze-kafka here is a stand-in with the broker's flags and its pprof
// listener, and nothing else: something for the lightness gate to weigh. With
// FIXTURE_BALLAST=<bytes> it holds that much more heap, which is how the gate's
// test plants a heavier broker.
package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
)

var ballast []byte

func main() {
	pprofAddr := flag.String("pprof-addr", "", "")
	for _, name := range []string{"listen", "console-addr", "schema-registry-addr", "data-dir"} {
		flag.String(name, "", "")
	}
	flag.Parse()
	if n, _ := strconv.Atoi(os.Getenv("FIXTURE_BALLAST")); n > 0 {
		ballast = make([]byte, n)
		for i := range ballast {
			ballast[i] = 1
		}
	}
	if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
		os.Exit(1)
	}
}

// Command release builds the kafka module for every platform and writes its
// release layout (archives, index.yaml, meta.yaml) under --out. It is the same
// SDK library every module repo releases with; there is nothing else to it.
//
//	go run ./release --out dist            # build all platforms
//	go run ./release version               # print the version in module.yaml
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/doze-dev/doze-sdk/modtool"

	kafka "github.com/doze-dev/doze-kafka/module"
)

const name = "kafka"

func main() {
	version, err := moduleVersion("module.yaml")
	if err != nil {
		fail(err)
	}
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	out := flag.String("out", "dist", "output directory (release layout)")
	triples := flag.String("triples", "", "comma-separated platform triples (default: all)")
	flag.Parse()

	build := modtool.AllTriples()
	if *triples != "" {
		build = strings.Split(*triples, ",")
	}
	m := modtool.Module{Name: name, Version: version, Namespace: "doze", PluginPath: "./plugin", Driver: kafka.New()}
	if err := modtool.Release(m, *out, build); err != nil {
		fail(err)
	}
}

func moduleVersion(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var f struct {
		Version string `yaml:"version"`
	}
	if err := yaml.Unmarshal(b, &f); err != nil || f.Version == "" {
		return "", fmt.Errorf("%s: no version", path)
	}
	return f.Version, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

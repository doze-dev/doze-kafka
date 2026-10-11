//go:build faultinject

package faultinject

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The points. (The packages that hit them spell the names out, because they
// must compile without this package.)
const (
	LogWrite  = "log.write"
	LogSync   = "log.sync"
	MetaWrite = "meta.write"
)

// Fault is what a point does when it is hit.
type Fault struct {
	Err   error         // returned from the hit, after any delay.
	Delay time.Duration // slept before returning.
	After int           // hits that pass untouched before the fault starts.
}

type armed struct {
	Fault
	hits int
}

var (
	mu      sync.Mutex
	faults  = map[string]*armed{}
	file    string
	fileMod time.Time
	fileSz  int64
	fileSet map[string]*armed
)

func init() {
	if spec := os.Getenv("DOZE_FAULTS"); spec != "" {
		set, err := Parse(spec)
		if err != nil {
			fmt.Fprintln(os.Stderr, "faultinject: DOZE_FAULTS:", err)
			os.Exit(2)
		}
		for p, f := range set {
			faults[p] = &armed{Fault: f}
		}
	}
	file = os.Getenv("DOZE_FAULTS_FILE")
}

// Set arms a point and returns a func that disarms it.
func Set(point string, f Fault) (clear func()) {
	mu.Lock()
	faults[point] = &armed{Fault: f}
	mu.Unlock()
	return func() {
		mu.Lock()
		delete(faults, point)
		mu.Unlock()
	}
}

// Reset disarms every point.
func Reset() {
	mu.Lock()
	faults = map[string]*armed{}
	mu.Unlock()
}

// Hit is called at a point. It returns the armed fault's error, or nil.
func Hit(point string) error {
	mu.Lock()
	a := faults[point]
	if a == nil && file != "" {
		a = fromFileLocked(point)
	}
	if a == nil {
		mu.Unlock()
		return nil
	}
	a.hits++
	pass := a.hits <= a.After
	delay, err := a.Delay, a.Err
	mu.Unlock()
	if pass {
		return nil
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return err
}

func fromFileLocked(point string) *armed {
	st, err := os.Stat(file)
	if err != nil {
		fileSet = nil
		fileMod, fileSz = time.Time{}, 0
		return nil
	}
	if !st.ModTime().Equal(fileMod) || st.Size() != fileSz {
		fileMod, fileSz = st.ModTime(), st.Size()
		fileSet = nil
		if b, err := os.ReadFile(file); err == nil {
			if set, err := Parse(string(b)); err == nil {
				fileSet = map[string]*armed{}
				for p, f := range set {
					fileSet[p] = &armed{Fault: f}
				}
			}
		}
	}
	return fileSet[point]
}

// Parse reads the DOZE_FAULTS syntax described in the package comment.
func Parse(spec string) (map[string]Fault, error) {
	out := map[string]Fault{}
	for _, item := range strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == '\n' }) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		point, action, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("%q: want point=action", item)
		}
		var f Fault
		if body, n, ok := strings.Cut(action, "@"); ok {
			after, err := strconv.Atoi(n)
			if err != nil || after < 0 {
				return nil, fmt.Errorf("%q: bad hit count after @", item)
			}
			f.After, action = after, body
		}
		kind, arg, _ := strings.Cut(action, ":")
		switch kind {
		case "error":
			if arg == "" {
				arg = "injected fault"
			}
			f.Err = errors.New(arg)
		case "enospc":
			f.Err = &os.PathError{Op: "write", Path: point, Err: syscall.ENOSPC}
		case "delay":
			d, err := time.ParseDuration(arg)
			if err != nil {
				return nil, fmt.Errorf("%q: %v", item, err)
			}
			f.Delay = d
		default:
			return nil, fmt.Errorf("%q: unknown action %q (error, enospc, delay)", item, kind)
		}
		out[strings.TrimSpace(point)] = f
	}
	return out, nil
}

package console

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// The static files are embedded and unversioned in their URLs, so they cannot be
// cached "forever". They are cached with revalidation instead: every file carries
// an ETag derived from its content, and a repeat visit costs a 304, not the body
// (CodeMirror alone is a few hundred kilobytes).
var (
	etagOnce sync.Once
	etags    map[string]string
)

func staticETags() map[string]string {
	etagOnce.Do(func() {
		etags = map[string]string{}
		fs.WalkDir(staticFS, "static", func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if b, err := staticFS.ReadFile(p); err == nil {
				sum := sha256.Sum256(b)
				etags[p] = `"` + hex.EncodeToString(sum[:8]) + `"`
			}
			return nil
		})
	})
	return etags
}

func (c *Console) staticHandler() http.Handler {
	if c.dev != "" {
		// Dev mode: straight from disk, never cached.
		// Rooted at static/ itself, so "static/../console.go" has nowhere to go: it
		// used to serve any file in the dev directory, source and templates included.
		files := http.StripPrefix("/static", http.FileServerFS(os.DirFS(filepath.Join(c.dev, "static"))))
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(path.Clean("/"+r.URL.Path), "/static/") {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Cache-Control", "no-store")
			setFontType(w, r.URL.Path)
			r2 := r.Clone(r.Context())
			r2.URL.Path = path.Clean("/" + r.URL.Path)
			files.ServeHTTP(w, r2)
		})
	}
	files := http.FileServerFS(staticFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The mux strips the console prefix, leaving "static/…".
		name := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if strings.HasSuffix(r.URL.Path, "/") || name == "static" {
			http.NotFound(w, r) // no directory listings.
			return
		}
		setFontType(w, name)
		if tag, ok := staticETags()[name]; ok {
			w.Header().Set("ETag", tag)
			w.Header().Set("Cache-Control", "no-cache")
			if r.Header.Get("If-None-Match") == tag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		files.ServeHTTP(w, r)
	})
}

// setFontType names the type of a font file. Go otherwise takes it from the
// operating system's table, which a minimal container or a Windows machine may
// not have an entry in; the pages are served with "nosniff", and a browser will
// not use a font whose type it has to guess.
func setFontType(w http.ResponseWriter, name string) {
	if strings.HasSuffix(name, ".woff2") {
		w.Header().Set("Content-Type", "font/woff2")
	}
}

package console

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"sync"
)

// The static files are embedded and unversioned in their URLs, so they cannot be
// cached "forever". They are cached with revalidation instead: every file carries
// an ETag derived from its content, and a repeat visit costs a 304, not the body
// (CodeMirror alone is the better part of a megabyte).
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

func staticHandler() http.Handler {
	files := http.FileServerFS(staticFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The mux strips the console prefix, leaving "static/…".
		name := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
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

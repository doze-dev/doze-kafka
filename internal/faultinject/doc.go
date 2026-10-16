// Package faultinject is the fault seam: named points in the log and the
// metadata store where a test can make a write fail, a sync fail or a sync
// crawl, in this process or, through an environment variable, in the built
// binary.
//
// It exists only under the build tag "faultinject". Every other file in this
// package carries that tag, and the two packages that have points (the log and
// the metadata store) reach it through a pair of tiny files, one per tag state,
// so a release build links none of it: a test greps the binary's symbol table
// for the package path.
//
// Points: "log.write" (appending a batch to a segment), "log.sync" (flushing a
// segment, which acks=all and the timer both do), "meta.write" (a metadata
// transaction), "log.fsync" (an fsync call itself failing, as opposed to "log.sync",
// which fails the request before any fsync is issued: only the former takes a
// partition offline, as a real failed fsync does). The log's existing
// ManagerOptions.BeforeSync hook is the per-manager case of "log.sync": it runs at
// the same place, after the global fault.
//
// Every other operation the log makes on the file system is a point too, hit at
// the log's file seam (internal/storage/log/fs.go): "log.dirsync" (flushing a
// directory), "log.rename", "log.create", "log.remove", "log.truncate",
// "log.mkdir", and "log.file.write" (a write to anything that is not a batch
// appended to a segment: an index entry, a cleaned segment, one of the small
// files). "log.fsync" is every file fsync the log makes, of a segment or of one
// of those. "log.read" is a read of a segment's records (a fetch).
//
// Arming from the environment (DOZE_FAULTS, and DOZE_FAULTS_FILE, which is read
// again on every hit so a test can change the fault while the process runs):
//
//	log.sync=error:input/output error   fail every sync with that message
//	log.write=enospc                    fail every write with "no space left on device"
//	log.sync=delay:250ms                make every sync take 250ms longer
//	log.sync=error:disk gone@3          let three hits pass, then fail
//
// Several are separated by commas or newlines. An empty file clears them all.
package faultinject

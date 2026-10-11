//go:build faultinject

package log

import "github.com/doze-dev/doze-kafka/internal/faultinject"

func faultAt(point string) error { return faultinject.Hit(point) }

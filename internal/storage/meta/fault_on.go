//go:build faultinject

package meta

import "github.com/doze-dev/doze-kafka/internal/faultinject"

func faultAt(point string) error { return faultinject.Hit(point) }

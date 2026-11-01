package protocol

import (
	"reflect"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// populate gives every slice of structs under v one element and allocates every
// pointer to a struct, so that each ErrorCode field the type can have exists in the
// value. It returns the ErrorCode fields found, each as a settable value.
func populate(v reflect.Value, depth int, out *[]reflect.Value) {
	if depth > 16 {
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.Type().Elem().Kind() != reflect.Struct {
			return
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		populate(v.Elem(), depth+1, out)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Struct && v.Type().Elem().Kind() != reflect.Pointer {
			return
		}
		if v.Len() == 0 {
			v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		}
		for i := 0; i < v.Len(); i++ {
			populate(v.Index(i), depth+1, out)
		}
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if !t.Field(i).IsExported() {
				continue
			}
			f := v.Field(i)
			if t.Field(i).Name == "ErrorCode" && f.Kind() == reflect.Int16 {
				*out = append(*out, f)
				continue
			}
			populate(f, depth+1, out)
		}
	}
}

// Setting each ErrorCode field of every response type in turn, the recorder finds it.
// A depth cap of six hid five: OffsetFetch v8 and later (every consumer's fetch of its
// committed offsets), AddPartitionsToTxn v4 and later, and others (TRF-7).
func TestFirstErrorFindsEveryErrorCodeOfEveryResponse(t *testing.T) {
	checked := 0
	for key := int16(0); key < 200; key++ {
		q := kmsg.RequestForKey(key)
		if q == nil {
			continue
		}
		name := APIName(APIKey(key))
		var codes []reflect.Value
		resp := q.ResponseKind()
		populate(reflect.ValueOf(resp), 0, &codes)
		for i, f := range codes {
			f.SetInt(77)
			if got := firstError(resp); got != 77 {
				t.Errorf("%s: ErrorCode field %d of %d (%v) is set and firstError says %d", name, i+1, len(codes), f.Type(), got)
			}
			f.SetInt(0)
			checked++
		}
	}
	if checked < 100 {
		t.Errorf("only %d ErrorCode fields were exercised: the sweep is not covering the kmsg table", checked)
	}
}

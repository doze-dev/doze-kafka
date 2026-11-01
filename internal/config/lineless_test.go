package config

import (
	"errors"
	"testing"
)

// An error that names no line says none: no "line 0".
func TestAnErrorWithNoLineNamesNoLine(t *testing.T) {
	got := withoutValues(errors.New("something else")).Error()
	want := "toml:: this is not valid TOML (a string is written in quotes)" + notRepeated
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

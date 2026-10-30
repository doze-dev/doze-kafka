//go:build race

package schemaregistry

// raceOn is true under -race, which also puts coverage counters in atomic mode: the
// decoder runs about 75 times slower, and a bound set in seconds needs fewer values.
const raceOn = true

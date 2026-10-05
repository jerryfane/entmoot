//go:build race

package membership

// raceEnabled reports a -race build, whose instrumentation makes timings
// meaningless.
const raceEnabled = true

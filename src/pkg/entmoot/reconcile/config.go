package reconcile

// Config tunes the Session's recursion strategy.
//
// LeafThreshold is the maximum number of ids the Session will pack into a
// KindIDList leaf frame before preferring to split the range further with
// fingerprints. Very small values waste round-trips; very large values
// waste bandwidth on ranges that still differ.
//
// MaxRounds bounds the number of Next() invocations the Session will
// tolerate before returning ErrMaxRounds. It protects against pathological
// or adversarial peers who never converge.
//
// FanoutPerRound is the number of equal-width sub-ranges the Session splits
// a disagreeing KindFingerprint range into. Higher fanout reduces the
// number of round trips at the cost of larger frames per round.
type Config struct {
	LeafThreshold  int
	MaxRounds      int
	FanoutPerRound int
}

// DefaultConfig returns the conventional RBSR parameters used by Entmoot
// v1.2.1: 16-id leaves, 10 rounds, 16-way fanout. These mirror the
// defaults cited by Meyer et al. and the Negentropy reference
// implementation for groups in the 10^3-10^5 message range.
func DefaultConfig() Config {
	return Config{
		LeafThreshold:  16,
		MaxRounds:      10,
		FanoutPerRound: 16,
	}
}

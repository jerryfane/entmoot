package reconcile

import "errors"

// ErrMaxRounds is returned by Session.Next when the session has executed
// more steps than Config.MaxRounds allows. Reaching this error indicates
// either an adversarial peer or a bug — under normal operation RBSR
// converges in O(log(diff)) rounds.
var ErrMaxRounds = errors.New("reconcile: max rounds exceeded")

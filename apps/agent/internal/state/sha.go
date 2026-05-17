package state

import (
	"crypto/sha256"
	"hash"
)

// sha256New is a function value compatible with hkdf.New's first arg.
// Wrapping the constructor lets us swap in a test double if ever needed.
var sha256New = func() hash.Hash { return sha256.New() }

package wss

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackoff_Progression(t *testing.T) {
	b := NewBackoff()
	b.JitterPC = 0 // deterministic for the assertion

	require.Equal(t, 1*time.Second, b.Next())
	require.Equal(t, 2*time.Second, b.Next())
	require.Equal(t, 4*time.Second, b.Next())
	require.Equal(t, 8*time.Second, b.Next())
	require.Equal(t, 16*time.Second, b.Next())
	require.Equal(t, 32*time.Second, b.Next())
	require.Equal(t, 60*time.Second, b.Next(), "should cap at 60s")
	require.Equal(t, 60*time.Second, b.Next(), "should stay at cap")
}

func TestBackoff_Reset(t *testing.T) {
	b := NewBackoff()
	b.JitterPC = 0
	b.Next()
	b.Next()
	b.Reset()
	require.Equal(t, 1*time.Second, b.Next())
}

func TestBackoff_JitterStaysInBand(t *testing.T) {
	b := NewBackoff()
	// Force a known current value and re-check jitter range.
	const base = 10 * time.Second
	b.current = base
	for i := 0; i < 200; i++ {
		got := b.withJitter(base)
		require.GreaterOrEqual(t, got, time.Duration(float64(base)*0.8))
		require.LessOrEqual(t, got, time.Duration(float64(base)*1.2))
	}
}

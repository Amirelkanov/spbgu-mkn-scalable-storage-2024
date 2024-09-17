package main

import (
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	assert.True(t, slices.Contains(b[:], zero) && slices.Contains(b[:], one), "Byte array 'b' must contain zero and one bytes.")
}

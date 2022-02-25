package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var unsafeBytesToStringTestingSuite = []struct {
	In  []byte
	Exp string
}{
	{
		In:  nil,
		Exp: "",
	},
	{
		In:  []byte{},
		Exp: "",
	},
	{
		In:  []byte{'1', 'b'},
		Exp: "1b",
	},
	{
		In:  []byte{'1', 'b', 'c', 'f', '1'},
		Exp: "1bcf1",
	},
	{
		In:  []byte{'f', 'o', 'o', ' ', 'b', 'a', 'r'},
		Exp: "foo bar",
	},
}

func TestUnsafeBytesToString(t *testing.T) {
	for _, tt := range unsafeBytesToStringTestingSuite {
		t.Run("", func(t *testing.T) {
			exp := UnsafeBytesToString(tt.In)
			assert.Equal(t, tt.Exp, exp)
		})
	}
}

var unsafeStringToBytesTestingSuite = []struct {
	In  string
	Exp []byte
}{
	{
		In:  "",
		Exp: nil,
	},
	{
		In:  "1b",
		Exp: []byte{'1', 'b'},
	},
	{
		In:  "1bcf1",
		Exp: []byte{'1', 'b', 'c', 'f', '1'},
	},
	{
		In:  "foo bar",
		Exp: []byte{'f', 'o', 'o', ' ', 'b', 'a', 'r'},
	},
}

func TestUnsafeStringToBytes(t *testing.T) {
	for _, tt := range unsafeStringToBytesTestingSuite {
		t.Run("", func(t *testing.T) {
			exp := UnsafeStringToBytes(tt.In)
			assert.Equal(t, tt.Exp, exp)
		})
	}
}

package parser

import (
	"reflect"
	"unsafe"
)

// UnsafeBytesToString parses the given binary slice into a string using built-in unsafe package
//
// Took from: https://hackernoon.com/golang-unsafe-type-conversions-and-memory-access-odz3yrl
func UnsafeBytesToString(bytes []byte) string {
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	return *(*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: sliceHeader.Data,
		Len:  sliceHeader.Len,
	}))
}

// UnsafeStringToBytes parses the given string into a binary slice using built-in unsafe package
//
// Took from: https://hackernoon.com/golang-unsafe-type-conversions-and-memory-access-odz3yrl
func UnsafeStringToBytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: stringHeader.Data,
		Len:  stringHeader.Len,
		Cap:  stringHeader.Len,
	}))
}

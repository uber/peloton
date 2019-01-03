package randutil

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func choose(n int, choices string) []byte {
	b := make([]byte, n)
	for i := range b {
		c := choices[rand.Intn(len(choices))]
		b[i] = byte(c)
	}
	return b
}

const text = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Text returns randomly generated alphanumeric text of length n.
func Text(n int) []byte {
	return choose(n, text)
}

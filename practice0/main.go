package main

import (
	"fmt"
	"time"
)

func main() {
	b := [100]byte{}

	go func() {
		filler(b[:len(b)/2], '0', '1')
		time.Sleep(time.Second)
	}()

	go func() {
		filler(b[:len(b)/2], 'X', 'Y')
		time.Sleep(time.Second)
	}()

	go func() {
		fmt.Println(string(b[:]))
		time.Sleep(time.Second)
	}()

	select {}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := 0; i < 2; i++ {
		if i == 0 {
			b[i] = ifzero
		}
		if i == 1 {
			b[i] = ifnot
		}
	}
}

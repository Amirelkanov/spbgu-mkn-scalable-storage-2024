package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	var b [100]byte // Массив для изменения
	// Мьютекс для синхронизации (наверное, можно и каналом, заведя какой-нибудь ch bool для уверенности в том,
	// что только одна горутина владеет критической секцией, но зачем?)
	var mu sync.Mutex

	go func() {
		for {
			mu.Lock()
			filler(b[:len(b)/2], '0', '1')
			mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			mu.Lock()
			filler(b[len(b)/2:], 'X', 'Y')
			mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			mu.Lock()
			fmt.Println(string(b[:]))
			mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	for {
	}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := 0; i < len(b); i++ {
		if rand.Int()%2 == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}

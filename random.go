package simpledb

import (
	"math/rand"
	"sync"
	"time"
)

var (
	randomData  = rand.New(rand.NewSource(time.Now().Unix()))
	randomMutex sync.Mutex
)

func randUint64() uint64 {
	randomMutex.Lock()
	defer randomMutex.Unlock()
	return randomData.Uint64()
}

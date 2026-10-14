// Copyright 2026 gomaja. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ctxsync_test

import (
	"context"
	"fmt"
	"time"

	"github.com/gomaja/go-ctxsync"
)

func ExampleMutex_LockContext() {
	var mu ctxsync.Mutex
	counts := map[string]int{"item": 41}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mu.LockContext(ctx); err != nil {
		panic(err)
	}
	counts["item"]++
	value := counts["item"]
	mu.Unlock()

	fmt.Println(value)
	// Output: 42
}

func ExampleRWMutex_RLockContext() {
	var rw ctxsync.RWMutex
	counts := make(map[string]int)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rw.LockContext(ctx); err != nil {
		panic(err)
	}
	counts["item"] = 42
	rw.Unlock()

	if err := rw.RLockContext(ctx); err != nil {
		panic(err)
	}
	value := counts["item"]
	rw.RUnlock()

	fmt.Println(value)
	// Output: 42
}

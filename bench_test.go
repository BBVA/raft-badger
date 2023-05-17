/*
   Copyright 2018-2019 Banco Bilbao Vizcaya Argentaria, S.A.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package raftbadger

import (
	"os"
	"testing"

	"github.com/hashicorp/raft"
	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkBadgerStore_FirstIndex(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.FirstIndex(b, store)
}

func BenchmarkBadgerStore_LastIndex(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.LastIndex(b, store)
}

func BenchmarkBadgerStore_GetLog(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.GetLog(b, store)
}

func BenchmarkBadgerStore_StoreLog(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.StoreLog(b, store)
}

func BenchmarkBadgerStore_StoreLogs(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.StoreLogs(b, store)
}

func BenchmarkBadgerStore_DeleteRange(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.DeleteRange(b, store)
}

func BenchmarkBadgerStore_Set(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.Set(b, store)
}

func BenchmarkBadgerStore_Get(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.Get(b, store)
}

func BenchmarkBadgerStore_SetUint64(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.SetUint64(b, store)
}

func BenchmarkBadgerStore_GetUint64(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	raftbench.GetUint64(b, store)
}

func BenchmarkSet(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	for n := 0; n < b.N; n++ {
		store.Set(uint64ToBytes(uint64(n)), []byte("val"))
	}
}

func BenchmarkGet(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	for n := 0; n < b.N; n++ {
		store.Set(uint64ToBytes(uint64(n)), []byte("val"))
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		store.Get(uint64ToBytes(uint64(n)))
	}
}

func BenchmarkStoreLogs(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()
	for n := 0; n < b.N; n++ {
		store.StoreLogs([]*raft.Log{
			{
				Index: uint64(n),
				Term:  uint64(n),
			},
		})
	}
}

func BenchmarkGetLog(b *testing.B) {
	store, path := testBadgerStore(b)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()
	for n := 0; n < b.N; n++ {
		store.StoreLogs([]*raft.Log{
			{
				Index: uint64(n),
				Term:  uint64(n),
			},
		})
	}

	b.ResetTimer()

	ralog := new(raft.Log)
	for n := 0; n < b.N; n++ {
		store.GetLog(uint64(n), ralog)
	}
}

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
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

func testBadgerStore(t testing.TB) (*BadgerStore, string) {
	path, err := ioutil.TempDir("", "raftbadger")
	if err != nil {
		t.Fatalf("err. %s", err)
	}
	os.RemoveAll(path)

	// Successfully creates and returns a store
	badgerOpts := badger.DefaultOptions(path).WithLogger(nil)
	store, err := New(Options{
		Path:          path,
		NoSync:        true,
		BadgerOptions: &badgerOpts,
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store, path
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBadgerStore_Implements(t *testing.T) {
	var store interface{} = &BadgerStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("BadgerStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("BadgerStore does not implement raft.LogStore")
	}
}

func TestBadgerOptionsReadOnly(t *testing.T) {
	store, path := testBadgerStore(t)
	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}
	store.Close()

	defaultOpts := badger.DefaultOptions(path).WithLogger(nil)
	options := Options{
		Path:          path,
		BadgerOptions: &defaultOpts,
	}
	options.BadgerOptions.ReadOnly = true
	roStore, err := New(options)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer roStore.Close()

	result := new(raft.Log)
	if err := roStore.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back to same
	if !reflect.DeepEqual(log, result) {
		t.Errorf("bad: %v", result)
	}

	// Attempt to store the log, should fail on a read-only store
	err = roStore.StoreLog(log)
	if err != badger.ErrReadOnlyTxn {
		t.Errorf("expecting error %v, but got %v", badger.ErrReadOnlyTxn, err)
	}
}

func TestNewBadgerStore(t *testing.T) {
	store, path := testBadgerStore(t)

	// Ensure the directory was created
	if store.path != path {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure our files were created
	opts := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	db.Close()
}

func TestBadgerStore_FirstIndex(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestBadgerStore_LastIndex(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestBadgerStore_GetLog(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBadgerStore_SetLog(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBadgerStore_SetLogs(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestBadgerStore_DeleteRange(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestBadgerStore_Set_Get(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestBadgerStore_SetUint64_GetUint64(t *testing.T) {
	store, path := testBadgerStore(t)
	defer func() {
		store.Close()
		os.RemoveAll(path)
	}()

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

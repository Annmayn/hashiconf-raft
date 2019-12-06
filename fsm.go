package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

type fsm struct {
	mutex sync.Mutex
	//update: badgerdb instance
	// stateValue int
	db *badger.DB
}

type event struct {
	Type  string
	Value map[string]interface{}
}

// Apply applies a Raft log entry to the key-value store.
func (fsm *fsm) Apply(logEntry *raft.Log) interface{} {
	var e event
	if err := json.Unmarshal(logEntry.Data, &e); err != nil {
		panic("Failed unmarshaling Raft log entry. This is a bug.")
	}

	switch e.Type {
	case "set":
		fsm.mutex.Lock()
		defer fsm.mutex.Unlock()
		//update: store value in badgerdb
		// fsm.stateValue = e.Value
		// storeValue(fsm.stateValue, e.Value) //Todo
		// Start a writable transaction.

		// Use the transaction...
		for k := range e.Value {
			val, _ := json.Marshal(e.Value[k])
			fmt.Println(".....Saving key", k, "with val: ", string(val), "..........")
			txn := fsm.db.NewTransaction(true)
			defer txn.Discard()
			err := txn.Set([]byte(k), val)
			if err != nil {
				return err
			}
			// Commit the transaction and check for error.
			if err := txn.Commit(); err != nil {
				fmt.Println(".......Error: ", err, "........")
				return err
			}
			fmt.Println("Done")
		}
		fmt.Println("Value set")

		fmt.Println("Value commited")

		return nil
	default:
		panic(fmt.Sprintf("Unrecognized event type in Raft log entry: %s. This is a bug.", e.Type))
	}
}

func (fsm *fsm) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mutex.Lock()
	defer fsm.mutex.Unlock()

	return &fsmSnapshot{stateValue: -1}, nil
}

// Restore stores the key-value store to a previous state.
func (fsm *fsm) Restore(serialized io.ReadCloser) error {
	var snapshot fsmSnapshot
	if err := json.NewDecoder(serialized).Decode(&snapshot); err != nil {
		return err
	}

	// fsm.stateValue = snapshot.stateValue
	//todo : restore database
	return nil
}

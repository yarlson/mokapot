package store

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	sqsBucket = []byte("sqs")
	snsBucket = []byte("sns")
	stateKey  = []byte("state")
)

// BoltStore persists SQS and SNS engine state to a bbolt database file.
type BoltStore struct {
	db *bolt.DB
}

// Open opens (or creates) a bbolt database at the given path.
func Open(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	// Pre-create buckets.
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(sqsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(snsBucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create buckets: %w", err)
	}

	return &BoltStore{db: db}, nil
}

// SaveSQSState writes the SQS engine snapshot to the database.
func (s *BoltStore) SaveSQSState(data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(sqsBucket).Put(stateKey, data)
	})
}

// LoadSQSState reads the SQS engine snapshot from the database.
// Returns nil, nil if no snapshot exists.
func (s *BoltStore) LoadSQSState() ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(sqsBucket).Get(stateKey)
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// SaveSNSState writes the SNS engine snapshot to the database.
func (s *BoltStore) SaveSNSState(data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snsBucket).Put(stateKey, data)
	})
}

// LoadSNSState reads the SNS engine snapshot from the database.
// Returns nil, nil if no snapshot exists.
func (s *BoltStore) LoadSNSState() ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(snsBucket).Get(stateKey)
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// Close closes the database.
func (s *BoltStore) Close() error {
	return s.db.Close()
}

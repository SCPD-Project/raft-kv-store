package store

import (
	"github.com/boltdb/bolt"
	"time"
	"fmt"
)

type server struct {
	db *bolt.DB
}

func createNewServer(file string) (s *server, err error) {
	s = &server{}
	s.db, err = bolt.Open(file, 0600, &bolt.Options{Timeout: 1 * time.Second})
	s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("Cs244bBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	return
}

func(s *server) PersistPut(key, val []byte) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Cs244bBucket"))
		err := b.Put([]byte(key), []byte(val))
		return err
	})
	return err
}

func (s *server) PersistGet(key string) (ct string, err error) {
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Cs244bBucket"))
		r := b.Get([]byte(key))
		if r != nil {
			data := make([]byte, len(r))
			copy(data, r)
		}

		ct = string(r)
		return nil
	})
	return
}

func (s *server) PersistDelete(key string) (err error) {
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Cs244bBucket"))
		err := b.Delete([]byte(key))
		if err != nil {
			return fmt.Errorf("delete key failed: %s", err)
		}
		return nil
	})
	return
}

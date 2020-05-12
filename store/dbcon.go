package store

import (
	"github.com/boltdb/bolt"
	"time"
	"log"
	"fmt"
)

type kvDB struct {
	db *bolt.DB
	options bolt.Options
}

func NewDBConn(file string) (s *kvDB) {
	s = &kvDB{options: bolt.Options{Timeout: 1 * time.Second}}
	var err error
	s.db, err = bolt.Open(file, 0600, &s.options)
	if err != nil {
		log.Fatal(" error init boltdb: %s", err)
	}
	s.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(PersistentBucketName))
		if err != nil {
			log.Fatal(" error creating boltdb bucket: %s", err)
		}
		return nil
	})

	return
}

func(s *kvDB) Set(key, val string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(PersistentBucketName))
		err := b.Put([]byte(key), []byte(val))
		return err
	})
	return err
}

func (s *kvDB) Get(key string) (ct string, err error) {
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(PersistentBucketName))
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

func (s *kvDB) Delete(key string) (err error) {
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(PersistentBucketName))
		err := b.Delete([]byte(key))
		if err != nil {
			return fmt.Errorf("delete key failed: %s", err)
		}
		return nil
	})
	return
}
package store

import (
	"github.com/boltdb/bolt"
	"time"
)

type kvDB struct {
	db *bolt.DB
	options bolt.Options
}

func NewDBConn(file string, persistDir string) (s *kvDB, err error) {
	s = &kvDB{options: bolt.Options{Timeout: 1 * time.Second}}
	s.db, err = bolt.Open(file + persistDir, 0600, &s.options)
	if err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(PersistentBucketName))
		if err != nil {
			return err
		}
		return nil
	})

	return
}

func(s *kvDB) BatchSet(kv map[string]string)  error {
	err := s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(PersistentBucketName))

		for key, value := range kv {
			err := b.Put([]byte(key), []byte(value))
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (s *kvDB) FetchAllKeys(kv map[string]string) {
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(PersistentBucketName))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			kv[string(k)] = string(v)
		}

		return nil
	})
}

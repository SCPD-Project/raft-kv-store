package store

import (
	"github.com/boltdb/bolt"
	"time"
)

type persistKvDB struct {
	db *bolt.DB
	options bolt.Options
}

func newDBConn(file string, bucketName string) (s *persistKvDB, err error) {
	s = &persistKvDB{options: bolt.Options{Timeout: 1 * time.Second}}
	s.db, err = bolt.Open(file, 0600, &s.options)
	if err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return nil
	})

	return
}

func(f *fsmSnapshot) save() {
	err := f.persistDBConn.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(f.bucketName))

		for key, value := range f.store {
			err := b.Put([]byte(key), []byte(value))
			if err != nil {
				f.logger.Warnf(" Snapshot save failed for bucket: %s, " +
					"key: %s", f.bucketName, key)
				return err
			}
		}
		return nil
	}); if err != nil{
		f.logger.Warnf(" Snapshot persist failed for bucket: %s ", f.bucketName)
	}

	return
}

func (f *fsm) restore() (kv map[string]string) {
	if err := f.persistKvDbConn.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(f.persistBucketName))
		c := b.Cursor()
		kv = make(map[string]string)

		for k, v := c.First(); k != nil; k, v = c.Next() {
			kv[string(k)] = string(v)
		}

		return nil
	}); err != nil {
		f.log.Fatalf(" Snapshot restore failed from bucket: %s ", f.persistBucketName)
	}

	return kv
}

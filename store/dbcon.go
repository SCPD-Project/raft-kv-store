package store

import (
	"github.com/boltdb/bolt"
	log "github.com/sirupsen/logrus"
	"time"
)

type persistKvDB struct {
	db *bolt.DB
	options bolt.Options
	log *log.Entry
}

func newDBConn(file string, bucketName string, logger *log.Logger) (s *persistKvDB) {
	var err error
	l := logger.WithField("component", "persistStore")
	persistConn := &persistKvDB{options: bolt.Options{Timeout: 1 * time.Second}, log: l}
	persistConn.db, err = bolt.Open(file, 0600, &persistConn.options)
	if err != nil {
		l.Fatalf(" Failed creating persistent store with err: %s", err)
	}
	err = persistConn.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			l.Fatalf(" Error in creating bucket: %s with err: %s", bucketName, err)
		}
		return nil
	}); if err != nil {
		l.Fatalf(" Error on persist update conn with err: %s", err)
	}

	l.Infof("Successfully created persist db conn in directory %s, bucketName %s",
			file, bucketName)

	return persistConn
}

func(f *fsmSnapshot) save() {
	err := f.persistDBConn.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(f.bucketName))

		for key, value := range f.store {
			err := b.Put([]byte(key), []byte(value))
			if err != nil {
				f.persistDBConn.log.Warnf(" Snapshot save failed for bucket: %s, " +
					"key: %s", f.bucketName, key)
				return err
			}
		}
		return nil
	}); if err != nil{
		f.persistDBConn.log.Warnf(" Failed to persist snapshot for bucket: %s ", f.bucketName)
	}
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
		f.persistKvDbConn.log.Fatalf(" Snapshot restore failed from bucket: %s ", f.persistBucketName)
	}

	return kv
}

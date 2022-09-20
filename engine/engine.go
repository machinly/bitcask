package engine

import (
	"errors"
	"io"
	"time"

	"github.com/machinly/bitcask/engine/dbfile"
	"github.com/machinly/bitcask/engine/record"
)

type Engine interface {
	Put(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
	ListKeys() ([]string, error)
	Merge() error
	Sync() bool
	Close() bool
}

type indexSet struct {
	fileId        string
	valueSize     int64
	valuePosition int64
	tstamp        int64
}

type bitcask struct {
	index  map[string]indexSet
	dbFile dbfile.DBFile
}

func OpenBitcaskEngine(dirName string) (Engine, error) {
	dbFile, err := dbfile.OpenDBFile(dirName)
	if err != nil {
		return nil, err
	}
	bc := &bitcask{
		index:  make(map[string]indexSet),
		dbFile: dbFile,
	}

	err = bc.buildIndex()
	if err != nil {
		return nil, err
	}

	return bc, nil
}

func (c *bitcask) buildIndex() error {
	index := make(map[string]indexSet)
	deleteList := make([]string, 0)
	for _, fileName := range c.dbFile.FileList() {
		pos := int64(0)
		err := c.dbFile.ReadAll(fileName, func(ret int64, reader io.Reader) error {
			r, err := record.ParseRecord(reader)
			if err != nil {
				return err
			}
			if v, ok := index[r.Key()]; ok {
				if v.tstamp < r.Timestamp() {
					index[r.Key()] = indexSet{
						fileId:        fileName,
						valuePosition: pos + r.ValueRelativePosition(),
						valueSize:     r.ValueSize(),
						tstamp:        r.Timestamp(),
					}
				}
			} else {
				index[r.Key()] = indexSet{
					fileId:        fileName,
					valuePosition: ret + r.ValueRelativePosition(),
					valueSize:     r.ValueSize(),
					tstamp:        r.Timestamp(),
				}
			}
			pos += r.Len()
			return nil
		})
		if err != nil {
			return err
		}
	}
	for k, v := range index {
		if v.valueSize == 0 {
			deleteList = append(deleteList, k)
		}
	}
	for _, k := range deleteList {
		delete(index, k)
	}
	c.index = index
	return nil
}

func (c *bitcask) Put(key string, value string) error {
	r, err := record.NewRecord(key, value)
	if err != nil {
		return err
	}
	buf, err := r.ToBytes()
	if err != nil {
		return err
	}
	fileName, ret, err := c.dbFile.Write(buf)
	if err != nil {
		return err
	}
	c.index[key] = indexSet{
		fileId:        fileName,
		valuePosition: r.ValueRelativePosition() + ret,
		valueSize:     r.ValueSize(),
		tstamp:        time.Now().Unix(),
	}
	return nil
}

func (c *bitcask) Get(key string) (string, error) {
	vSet, ok := c.index[key]
	if !ok || vSet.valueSize == 0 {
		return "", errors.New("key not found")
	}
	buf := make([]byte, vSet.valueSize)
	n, err := c.dbFile.Read(vSet.fileId, vSet.valuePosition, buf)
	if err != nil {
		return "", err
	}
	if int64(n) != vSet.valueSize {
		return "", errors.New("read size not equal to value size")
	}
	return string(buf), nil
}

func (c *bitcask) Delete(key string) error {
	_, ok := c.index[key]
	if !ok {
		return errors.New("key not found")
	}
	delete(c.index, key)
	r, err := record.NewDeleteRecord(key)
	if err != nil {
		return err
	}
	buf, err := r.ToBytes()
	_, _, err = c.dbFile.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *bitcask) ListKeys() ([]string, error) {
	result := make([]string, 0, len(c.index))
	for k := range c.index {
		result = append(result, k)
	}
	return result, nil
}

func (c *bitcask) Merge() error {
	panic("not implemented")
}

func (c *bitcask) Sync() bool {
	err := c.dbFile.Sync()
	if err != nil {
		return false
	}
	return true
}

func (c *bitcask) Close() bool {
	err := c.dbFile.Close()
	if err != nil {
		return false
	}
	return true
}

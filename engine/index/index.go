package index

import (
	"errors"
	"io"

	"github.com/machinly/bitcask/engine/record"
)

type Index interface {
	Put(key string, value *Set) error
	Get(key string) (*Set, error)
	Delete(key string) error
}

type Set struct {
	FileId        string
	ValueSize     int64
	ValuePosition int64
	Tstamp        int64
}

type index struct {
	index map[string]*Set
}

func (i *index) Put(key string, value *Set) error {
	i.index[key] = value
	return nil
}

func (i *index) Get(key string) (*Set, error) {
	v, ok := i.index[key]
	if ok {
		return v, nil
	}
	return nil, errors.New("key not found")
}

func (i *index) Delete(key string) error {
	delete(i.index, key)
	return nil
}

func (i *index) ReadSetsFromDB() map[string]Set {
	index := make(map[string]Set)
	err := c.dbFile.ReadAll(fileName, func(ret int64, reader io.Reader) error {
		r, err := record.ParseRecord(reader)
		if err != nil {
			return err
		}
		if v, ok := index[r.Key()]; ok {
			if v.tstamp < r.Timestamp() {
				index[r.Key()] = index.
					NewSet(fileNamer.ValueSize(),
						pos+r.ValueRelativePosition(),
						r.Timestamp())
			}
		} else {
			index[r.Key()] = index.
				NewSet(fileNamer.ValueSize(),
					pos+r.ValueRelativePosition(),
					r.Timestamp())
		}
		pos += r.Len()
		return nil
	})
	if err != nil {
		return err
	}
	return index
}

func NewSet(fileName string, valueSize, valuePosition, tstamp int64) *Set {
	return &Set{
		fileId:        fileName,
		valueSize:     valueSize,
		valuePosition: valuePosition,
		tstamp:        tstamp,
	}
}

func NewSetFromRecord(fileName string, offset int64, r record.Record) *Set {
	return &Set{
		fileId:        fileName,
		valueSize:     r.ValueSize(),
		valuePosition: offset + r.ValueRelativePosition(),
		tstamp:        r.Timestamp(),
	}
}

func NewIndex() Index {
	return &index{
		index: make(map[string]*Set),
	}
}

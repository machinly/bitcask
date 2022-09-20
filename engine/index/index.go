package index

import (
	"errors"

	"github.com/machinly/bitcask/engine/record"
)

type Index interface {
	Put(key string, value *IndexSet) error
	Get(key string) (*IndexSet, error)
	Delete(key string) error
}

type IndexSet struct {
	fileId        string
	valueSize     int64
	valuePosition int64
	tstamp        int64
}

type index struct {
	index map[string]*IndexSet
}

func (i *index) Put(key string, value *IndexSet) error {
	i.index[key] = value
	return nil
}

func (i *index) Get(key string) (*IndexSet, error) {
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

func NewIndexSet(fileName string, valueSize, valuePosition, tstamp int64) *IndexSet {
	return &IndexSet{
		fileId:        fileName,
		valueSize:     valueSize,
		valuePosition: valuePosition,
		tstamp:        tstamp,
	}
}

func NewIndexSetFromRecord(fileName string, offset int64, r record.Record) *IndexSet {
	return &IndexSet{
		fileId:        fileName,
		valueSize:     r.ValueSize(),
		valuePosition: offset + r.ValueRelativePosition(),
		tstamp:        r.Timestamp(),
	}
}

func NewIndex() Index {
	return &index{
		index: make(map[string]*IndexSet),
	}
}

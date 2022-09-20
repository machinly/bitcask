package record

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"time"

	"github.com/machinly/bitcask/util"
)

type Record interface {
	ValueRelativePosition() int64
	ValueSize() int64
	Key() string
	Value() string
	Timestamp() int64
	ToBytes() ([]byte, error)
	Len() int64
	SetMeta(fileName string, offset int64)
}

const VER_SIZE = 1 // Version Size

// V1 RECORD
const (
	V1_CRC_SIZE    = 4 // CRC Size
	V1_TS_SIZE     = 8 // Timestamp Size
	V1_KS_SIZE     = 4 // Key Size
	V1_VS_SIZE     = 8 // Value Size
	V1_DF_SIZE     = 1 // Delete Flag Size
	V1_RECORD_SIZE = VER_SIZE + V1_CRC_SIZE + V1_TS_SIZE + V1_KS_SIZE + V1_VS_SIZE + V1_DF_SIZE

	V1_VERSION = 0x0       // Version 0000
	V1_DELETE  = byte(0x1) // delete flag 0001
)

type record struct {
	tStamp int64
	kSize  int32
	vSize  int64
	key    string
	value  string
	delete bool
	meta   struct {
		fileName string
		offset   int64
	}
}

func NewRecord(key string, value string) (Record, error) {
	return newRecord(key, value, time.Now().Unix(), false)
}

func NewDeleteRecord(key string) (Record, error) {
	return newRecord(key, "", time.Now().Unix(), true)
}

func newRecord(key string, value string, timestamp int64, delete bool) (Record, error) {
	if len(key) == 0 {
		return nil, errors.New("key is empty")
	}

	if len(key) > math.MaxInt32 {
		return nil, errors.New("key is too long")
	}
	if len(value) > math.MaxInt64 {
		return nil, errors.New("value is too long")
	}

	return &record{
		tStamp: timestamp,
		kSize:  int32(len(key)),
		vSize:  int64(len(value)),
		key:    key,
		value:  value,
		delete: delete,
	}, nil
}

func (r *record) ValueRelativePosition() int64 {
	return V1_RECORD_SIZE + int64(len(r.key))
}

func (r *record) ValueSize() int64 {
	return r.vSize
}

func (r *record) Key() string {
	return r.key
}

func (r *record) Value() string {
	return r.value
}

func (r *record) Timestamp() int64 {
	return r.tStamp
}

func (r *record) Len() int64 {
	return int64(V1_RECORD_SIZE + len(r.key) + len(r.value))
}

func (r *record) ToBytes() ([]byte, error) {
	// | crc 4b | tstamp 8b | key size 4b | value size 8b | key | value |
	buf := bytes.NewBuffer([]byte{})

	// tstamp
	buf.Write(util.Int64ToBytes(r.tStamp))

	// key size
	buf.Write(util.Int32ToBytes(r.kSize))

	// value size
	buf.Write(util.Int64ToBytes(r.vSize))

	// delete flag
	if r.delete {
		buf.Write([]byte{V1_DELETE})
	} else {
		buf.Write([]byte{0x0})
	}

	// key
	buf.Write([]byte(r.key))

	// value
	buf.Write([]byte(r.value))

	// crc sum
	crc := crc32.NewIEEE()
	_, err := crc.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	crcValue := crc.Sum32()

	result := bytes.NewBuffer([]byte{V1_VERSION})
	result.Write(util.Uint32ToBytes(crcValue))
	result.Write(buf.Bytes())

	if err != nil {
		return nil, err
	}
	return result.Bytes(), err
}

func (r *record) SetMeta(fileName string, offset int64) {
	r.meta.fileName = fileName
	r.meta.offset = offset
}

func (r *record) GetMeta() (string, int64) {
	return r.meta.fileName, r.meta.offset
}

func ParseRecord(reader io.Reader) (Record, error) {
	// | crc 4b | tstamp 8b | key size 4b | value size 8b | del flag 1b | key | value |
	head := make([]byte, V1_RECORD_SIZE)
	_, err := reader.Read(head)
	if err != nil {
		return nil, err
	}

	offset := 0
	version := util.BytesToUint32(head[offset : offset+VER_SIZE])
	offset += VER_SIZE
	if version != V1_VERSION {
		return nil, errors.New("version error")
	}

	crc := util.BytesToUint32(head[offset : offset+V1_CRC_SIZE])
	offset += V1_CRC_SIZE

	tstamp := util.BytesToInt64(head[offset : offset+V1_TS_SIZE])
	offset += V1_TS_SIZE

	keySize := util.BytesToInt32(head[offset : offset+V1_KS_SIZE])
	offset += V1_KS_SIZE

	valueSize := util.BytesToInt64(head[offset : offset+V1_VS_SIZE])
	offset += V1_VS_SIZE

	deleteFlagByte := head[offset : offset+V1_DF_SIZE]
	deleteFlag := false
	if deleteFlagByte[0]&V1_DELETE == V1_DELETE {
		deleteFlag = true
	}

	// get key
	key := make([]byte, keySize)
	_, err = reader.Read(key)

	// get value
	value := make([]byte, valueSize)
	_, err = reader.Read(value)

	// check crc
	buf := bytes.NewBuffer([]byte{})
	buf.Write(head[VER_SIZE+V1_CRC_SIZE:])
	buf.Write(key)
	buf.Write(value)
	crcSum := crc32.ChecksumIEEE(buf.Bytes())
	if crcSum != crc {
		return nil, errors.New("crc check error")
	}
	return newRecord(string(key), string(value), tstamp, deleteFlag)
}

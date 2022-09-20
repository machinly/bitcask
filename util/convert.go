package util

func Int64ToBytes(n int64) []byte {
	buf := make([]byte, 8)
	for i := range buf {
		buf[i] = byte(n | 0x00)
		n >>= 8
	}
	return buf
}

func Int32ToBytes(n int32) []byte {
	buf := make([]byte, 4)
	for i := range buf {
		buf[i] = byte(n | 0x00)
		n >>= 8
	}
	return buf
}

func Uint32ToBytes(n uint32) []byte {
	buf := make([]byte, 4)
	for i := range buf {
		buf[i] = byte(n | 0x00)
		n >>= 8
	}
	return buf
}

func BytesToUint32(buf []byte) uint32 {
	var n uint32
	for i := range buf {
		n |= uint32(buf[i]) << uint(i*8)
	}
	return n
}

func BytesToInt64(buf []byte) int64 {
	var n int64
	for i := range buf {
		n |= int64(buf[i]) << uint(i*8)
	}
	return n
}

func BytesToInt32(buf []byte) int32 {
	var n int32
	for i := range buf {
		n |= int32(buf[i]) << uint(i*8)
	}
	return n
}

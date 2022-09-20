package util

import (
	"math"
	"reflect"
	"testing"
)

func TestBytesToInt32(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "empty bytes to int32",
			args: args{buf: []byte{}},
			want: 0,
		},
		{
			name: "little bytes to int32",
			args: args{buf: []byte{0x7b, 0x00, 0x00, 0x00}},
			want: 123,
		},
		{
			name: "max bytes to int32",
			args: args{buf: []byte{0xFF, 0xFF, 0xFF, 0x7F}},
			want: math.MaxInt32,
		},
		{
			name: "neg max bytes to neg int32",
			args: args{buf: []byte{0x00, 0x00, 0x00, 0x80}},
			want: math.MinInt32,
		},
		{
			name: "overflow bytes to int32",
			args: args{buf: []byte{0x00, 0x00, 0x00, 0x00, 0xFF}},
			want: 0,
		},
		{
			name: "nil bytes to int32",
			args: args{buf: nil},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BytesToInt32(tt.args.buf); got != tt.want {
				t.Errorf("BytesToInt32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBytesToInt64(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "empty bytes to int64",
			args: args{buf: []byte{}},
			want: 0,
		},
		{
			name: "little bytes to int64",
			args: args{buf: []byte{0x7b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			want: 123,
		},
		{
			name: "max bytes to int64",
			args: args{buf: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
			want: math.MaxInt64,
		},
		{
			name: "neg max bytes to neg int64",
			args: args{buf: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}},
			want: math.MinInt64,
		},
		{
			name: "overflow bytes to int64",
			args: args{buf: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF}},
			want: 0,
		},
		{
			name: "nil bytes to int64",
			args: args{buf: nil},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BytesToInt64(tt.args.buf); got != tt.want {
				t.Errorf("BytesToInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBytesToUint32(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "empty bytes to uint32",
			args: args{buf: []byte{}},
			want: 0,
		},
		{
			name: "little bytes to uint32",
			args: args{buf: []byte{0x7b, 0x00, 0x00, 0x00}},
			want: 123,
		},
		{
			name: "max bytes to uint32",
			args: args{buf: []byte{0xFF, 0xFF, 0xFF, 0xFF}},
			want: math.MaxUint32,
		},
		{
			name: "overflow bytes to uint32",
			args: args{buf: []byte{0x00, 0x00, 0x00, 0x00, 0xFF}},
			want: 0,
		},
		{
			name: "nil bytes to uint32",
			args: args{buf: nil},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BytesToUint32(tt.args.buf); got != tt.want {
				t.Errorf("BytesToUint32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInt32ToBytes(t *testing.T) {
	type args struct {
		n int32
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "int 123 to bytes",
			args: args{n: 123},
			want: []byte{123, 0, 0, 0},
		},
		{
			name: "int 256 to bytes",
			args: args{n: 256},
			want: []byte{0x00, 0x01, 0, 0},
		},
		{
			name: "int 512 to bytes",
			args: args{n: 512},
			want: []byte{0x00, 0x02, 0, 0},
		},
		{
			name: "int -1 to bytes",
			args: args{n: -1},
			want: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "int to bytes",
			args: args{n: 0},
			want: []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name: "int Max to bytes",
			args: args{n: math.MaxInt32},
			want: []byte{0xFF, 0xFF, 0xFF, 0x7F},
		},
		{
			name: "int Min to bytes",
			args: args{n: math.MinInt32},
			want: []byte{0x00, 0x00, 0x00, 0x80},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Int32ToBytes(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInt64ToBytes(t *testing.T) {
	type args struct {
		n int64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "int64 123 to bytes",
			args: args{n: 123},
			want: []byte{123, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "int64 256 to bytes",
			args: args{n: 256},
			want: []byte{0x00, 0x01, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "int64 zero to bytes",
			args: args{n: 0},
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name: "int64 -1 to bytes",
			args: args{n: -1},
			want: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "int Max int64 to bytes",
			args: args{n: math.MaxInt64},
			want: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		},
		{
			name: "int Min int64 to bytes",
			args: args{n: math.MinInt64},
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Int64ToBytes(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Int64ToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUint32ToBytes(t *testing.T) {
	type args struct {
		n uint32
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "uint 123 to bytes",
			args: args{n: 123},
			want: []byte{0x7b, 0x00, 0x00, 0x00},
		},
		{
			name: "uint 256 to bytes",
			args: args{n: 256},
			want: []byte{0x00, 0x01, 0x00, 0x00},
		},
		{
			name: "uint 512 to bytes",
			args: args{n: 512},
			want: []byte{0x00, 0x02, 0x00, 0x00},
		},
		{
			name: "uint Max to bytes",
			args: args{n: math.MaxUint32},
			want: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name: "uint Min to bytes",
			args: args{n: 0},
			want: []byte{0x00, 0x00, 0x00, 0x00},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Uint32ToBytes(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("uint32ToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

package dbfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	MAX_FILE_SIZE = 100 * 1024 * 1024 // 100MB
)

type DBFile interface {
	Write(p []byte) (fileName string, startPos int64, err error)
	Read(fileName string, offset int64, p []byte) (n int, err error)
	ReadAll(fileName string, readFunc func(int64, io.Reader) error) (err error)
	Close() error
	Sync() error
	FileList() []string
	CurrentFile() string
	Remove(fileName string) error
}

type dbFile struct {
	fileMap     map[string]*os.File
	currentFile *os.File
	dir         string
}

func OpenDBFile(dir string) (DBFile, error) {
	newDbFileName := fmt.Sprintf("data-%d.db", time.Now().Unix())
	currentFile, err := openWriteFile(dir, newDbFileName)
	if err != nil {
		return nil, err
	}
	fileMap, err := openReadFiles(dir, newDbFileName)
	if err != nil {
		return nil, err
	}
	return &dbFile{
		fileMap:     fileMap,
		currentFile: currentFile,
		dir:         dir,
	}, nil
}

func openReadFiles(dirName, newDbFileName string) (map[string]*os.File, error) {
	files := make(map[string]*os.File)
	filepaths, err := filepath.Glob(dirName + "/*.db")
	if err != nil {
		return nil, err
	}
	for _, fp := range filepaths {
		file, err := openReadFile(fp)
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if stat.Size() == 0 && stat.Name() != newDbFileName {
			err := file.Close()
			if err != nil {
				return nil, err
			}
			err = os.Remove(file.Name())
			if err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		files[file.Name()] = file
	}

	if err != nil {
		return nil, err
	}
	return files, nil
}

func openReadFile(fp string) (*os.File, error) {
	abs, err := filepath.Abs(fp)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(abs, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func openWriteFile(dirName string, fileId string) (*os.File, error) {
	abs, err := filepath.Abs(dirName)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(abs, 0755)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(abs, fileId), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (db *dbFile) Write(p []byte) (fileName string, startPos int64, err error) {
	stat, err := db.currentFile.Stat()
	if err != nil {
		return "", 0, err
	}
	ret := stat.Size()
	if ret > MAX_FILE_SIZE {
		newDbFileName := fmt.Sprintf("data-%d.db", time.Now().Unix())
		err := db.currentFile.Close()
		if err != nil {
			return "", 0, err
		}

		db.currentFile, err = openWriteFile(db.dir, newDbFileName)
		if err != nil {
			return "", 0, err
		}
		ret = 0

		file, err := openReadFile(filepath.Join(db.dir, newDbFileName))
		if err != nil {
			return "", 0, err
		}
		db.fileMap[file.Name()] = file
	}
	_, err = db.currentFile.Write(p)
	if err != nil {
		return "", 0, err
	}
	return db.currentFile.Name(), ret, err
}

func writeFile(f *os.File, p []byte) (int, error) {
	return f.Write(p)
}

func (db *dbFile) Read(fileName string, offset int64, p []byte) (n int, err error) {
	if f, ok := db.fileMap[fileName]; ok {
		seek, err := f.Seek(offset, 0)
		if err != nil {
			return 0, err
		}
		if seek != offset {
			return 0, fmt.Errorf("seek error")
		}
		n, err = f.Read(p)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	return 0, fmt.Errorf("file not found")
}

func (db *dbFile) ReadAll(fileName string, readFunc func(int64, io.Reader) error) (err error) {
	if f, ok := db.fileMap[fileName]; ok {
		ret, err := f.Seek(0, 0)
		stats, err := f.Stat()
		if err != nil {
			return err
		}
		for stats.Size() > ret {
			if err != nil {
				return err
			}
			err = readFunc(ret, f)
			if err != nil {
				return err
			}
			ret, err = f.Seek(0, 1)
		}
		return nil
	}
	return fmt.Errorf("file not found")
}

func (db *dbFile) Close() error {
	err := db.currentFile.Close()
	if err != nil {
		return err
	}
	for _, file := range db.fileMap {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *dbFile) Sync() error {
	err := db.currentFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (db *dbFile) FileList() []string {
	list := make([]string, 0, len(db.fileMap))
	for s := range db.fileMap {
		list = append(list, s)
	}
	return list
}

func (db *dbFile) CurrentFile() string {
	return db.currentFile.Name()
}

func (db *dbFile) Remove(fileName string) error {
	err := os.Remove(fileName)
	if err != nil {
		return err
	}
	delete(db.fileMap, fileName)
	return nil
}

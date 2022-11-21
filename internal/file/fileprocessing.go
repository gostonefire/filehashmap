package file

import (
	"encoding/binary"
	"fmt"
	"github.com/gostonefire/filehashmap/internal/conf"
	"github.com/gostonefire/filehashmap/internal/model"
	"io"
	"os"
)

// SCFilesConf - Is a struct to be passed in the call to NewSCFiles and contains configuration that affects
// file processing.
//   - mapFileName is the name of the map file to create
//   - ovflFileName is the name of the overflow file to create
//   - keyLength is the fixed length of keys to store
//   - valueLength is the fixed length of values to store
//   - recordsPerBucket is the number of records available for use in each bucket
//   - header is a struct containing data to write to the map file header section
//   - fileSize is the size of the map file to create
type SCFilesConf struct {
	MapFileName      string
	OvflFileName     string
	KeyLength        int64
	ValueLength      int64
	RecordsPerBucket int64
	FileSize         int64
}

// SCFiles - Represents an implementation of file support for the Separate Chaining Collision Resolution Technique.
// It uses two files in this particular implementation where one stores directly addressable buckets and the
// other manages overflow in single linked lists.
type SCFiles struct {
	mapFileName      string
	ovflFileName     string
	mapFile          *os.File
	ovflFile         *os.File
	keyLength        int64
	valueLength      int64
	recordsPerBucket int64
	mapFileSize      int64
}

// NewSCFiles - Returns a pointer to a new instance of Separate Chaining file implementation.
// It always creates new files (or opens and truncate existing files)
//   - scFilesConf is a struct providing configuration parameter affecting files creation and processing (see SCFilesConf)
//   - header is a struct containing data to write to the map file header section
//
// It returns:
//   - scFiles which is a pointer to the created instance
//   - err which is a standard Go type of error
func NewSCFiles(scFilesConf SCFilesConf, header model.Header) (scFiles *SCFiles, err error) {
	scFiles = &SCFiles{
		mapFileName:      scFilesConf.MapFileName,
		ovflFileName:     scFilesConf.OvflFileName,
		keyLength:        scFilesConf.KeyLength,
		valueLength:      scFilesConf.ValueLength,
		recordsPerBucket: scFilesConf.RecordsPerBucket,
		mapFileSize:      scFilesConf.FileSize,
	}

	err = scFiles.createNewHashMapFile(header)
	if err != nil {
		return
	}
	err = scFiles.createNewOverflowFile()
	if err != nil {
		return
	}

	return
}

// NewSCFilesFromExistingFiles - Returns a pointer to a new instance of Separate Chaining file implementation given
// existing files. If files doesn't exist, doesn't have a valid header or if its file size seems wrong given
// size from header it fails with error.
//   - mapFileName is the filename of an existing map file
//   - ovflFileName is the filename of an existing overflow file
//
// It returns:
//   - scFiles which is a pointer to the created instance
//   - header which is a struct containing data read from the map file header section
//   - err which is a standard Go type of error
func NewSCFilesFromExistingFiles(mapFileName, ovflFileName string) (scFiles *SCFiles, header model.Header, err error) {
	scFiles = &SCFiles{mapFileName: mapFileName, ovflFileName: ovflFileName}

	header, err = scFiles.openHashMapFile()
	if err != nil {
		return
	}
	err = scFiles.openOverflowFile()
	if err != nil {
		return
	}

	scFiles.keyLength = header.KeyLength
	scFiles.valueLength = header.ValueLength
	scFiles.recordsPerBucket = header.RecordsPerBucket

	return
}

// getHeader - Reads header data from file and returns it as a Header struct
func (S *SCFiles) getHeader() (header model.Header, err error) {
	_, err = S.mapFile.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, conf.MapFileHeaderLength)
	_, err = S.mapFile.Read(buf)
	if err != nil {
		return
	}

	header = bytesToHeader(buf)

	return
}

// setHeader - Takes a Header struct and writes header data to file
func (S *SCFiles) setHeader(header model.Header) (err error) {
	_, err = S.mapFile.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	buf := headerToBytes(header)

	_, err = S.mapFile.Write(buf)

	return
}

// GetBucketRecords - Returns all records for a given bucket number in a Bucket struct
func (S *SCFiles) GetBucketRecords(bucketNo int64) (bucket model.Bucket, err error) {
	trueRecordLength := S.keyLength + S.valueLength + conf.InUseFlagBytes
	bucketAddress := conf.MapFileHeaderLength + bucketNo*(trueRecordLength*S.recordsPerBucket+conf.BucketHeaderLength)

	_, err = S.mapFile.Seek(bucketAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength*S.recordsPerBucket+conf.BucketHeaderLength)
	_, err = S.mapFile.Read(buf)
	if err != nil {
		return
	}

	bucket, err = bytesToBucket(buf, bucketAddress, S.keyLength, S.valueLength, S.recordsPerBucket)

	return
}

// SetBucketRecord - Sets a bucket record in the hash map file
func (S *SCFiles) SetBucketRecord(record model.Record) (err error) {
	buf := make([]byte, 1, S.keyLength+S.valueLength+conf.InUseFlagBytes)
	if record.InUse {
		buf[0] = 1
	}
	buf = append(buf, record.Key...)
	buf = append(buf, record.Value...)

	_, err = S.mapFile.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = S.mapFile.Write(buf)

	return
}

// openHashMapFile - Opens the hash map file and does some rudimentary checks of its validity
func (S *SCFiles) openHashMapFile() (header model.Header, err error) {
	if stat, ok := os.Stat(S.mapFileName); ok == nil {
		S.mapFile, err = os.OpenFile(S.mapFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing hash map file: %s", err)
			return
		}

		header, err = S.getHeader()
		if err != nil {
			_ = S.mapFile.Close()
			S.mapFile = nil
			err = fmt.Errorf("unable to read header from hash map file: %s", err)
			return
		}

		if stat.Size() != header.FileSize {
			_ = S.mapFile.Close()
			S.mapFile = nil
			err = fmt.Errorf("actual file size doesn't conform with header indicated file size")
			return
		}
	} else {
		err = fmt.Errorf("hash map file not found")
		return
	}

	return
}

// openOverflowFile - Opens the overflow file and does som rudimentary checks of its validity
func (S *SCFiles) openOverflowFile() (err error) {
	if stat, ok := os.Stat(S.ovflFileName); ok == nil {
		S.ovflFile, err = os.OpenFile(S.ovflFileName, os.O_RDWR, 0644)
		if err != nil {
			err = fmt.Errorf("unable to open existing overflow file: %s", err)
			return
		}

		if stat.Size() < conf.OvflFileHeaderLength {
			_ = S.ovflFile.Close()
			S.ovflFile = nil
			err = fmt.Errorf("actual file size is smaller than minimum overflow file size")
			return
		}
	} else {
		err = fmt.Errorf("overflow file not found")
		return
	}

	return
}

// createNewHashMapFile - Creates a new hash map file. If it already exists it will first be truncated to zero length
// and then to expected length, hence deleting all existing data.
func (S *SCFiles) createNewHashMapFile(header model.Header) (err error) {
	S.mapFile, err = os.OpenFile(S.mapFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new map file: %s", err)
		return
	}
	err = S.mapFile.Truncate(S.mapFileSize)
	if err != nil {
		_ = S.mapFile.Close()
		S.mapFile = nil
		err = fmt.Errorf("error while truncate new map file to length %d: %s", S.mapFileSize, err)
		return
	}

	err = S.setHeader(header)
	if err != nil {
		err = fmt.Errorf("error while writing header to map file: %s", err)
		return
	}

	return
}

// createNewOverflowFile - Creates a new overflow file. If it already exists it will first be truncated to zero length
// and then to expected length, hence deleting all existing data.
func (S *SCFiles) createNewOverflowFile() (err error) {
	S.ovflFile, err = os.OpenFile(S.ovflFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		err = fmt.Errorf("error while open/create new overflow file: %s", err)
		return
	}
	err = S.ovflFile.Truncate(conf.OvflFileHeaderLength)
	if err != nil {
		_ = S.ovflFile.Close()
		S.ovflFile = nil
		err = fmt.Errorf("error while truncate new overflow file to length %d: %s", conf.OvflFileHeaderLength, err)
	}

	return
}

// CloseFiles - Closes the map files
func (S *SCFiles) CloseFiles() {
	if S.ovflFile != nil {
		_ = S.ovflFile.Sync()
		_ = S.ovflFile.Close()
	}

	if S.mapFile != nil {
		_ = S.mapFile.Sync()
		_ = S.mapFile.Close()
	}
}

// RemoveFiles - Removes the map files, make sure to close them first before calling this function
func (S *SCFiles) RemoveFiles() (err error) {
	// Only try to remove if exists, and are not by accident directories (could happen when testing things out)
	if stat, ok := os.Stat(S.ovflFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(S.ovflFileName)
			if err != nil {
				err = fmt.Errorf("error while removing overflow file: %s", err)
				return
			}
		}
	}
	if stat, ok := os.Stat(S.mapFileName); ok == nil {
		if !stat.IsDir() {
			err = os.Remove(S.mapFileName)
			if err != nil {
				err = fmt.Errorf("error while removing map file: %s", err)
				return
			}
		}
	}

	return
}

// NewBucketOverflow - Adds a new overflow record to a file.
func (S *SCFiles) NewBucketOverflow(key, value []byte) (overflowAddress int64, err error) {
	overflowAddress, err = S.ovflFile.Seek(0, io.SeekEnd)
	if err != nil {
		return
	}

	buf := make([]byte, conf.OverflowAddressLength+conf.InUseFlagBytes, S.keyLength+S.valueLength+conf.OverflowAddressLength)
	buf[conf.OverflowAddressLength] = conf.RecordInUse
	buf = append(buf, key...)
	buf = append(buf, value...)

	_, err = S.ovflFile.Write(buf)
	if err != nil {
		return
	}

	return
}

// SetBucketOverflowAddress - Sets the overflow address for a bucket identified by its address in file
func (S *SCFiles) SetBucketOverflowAddress(bucketAddress, overflowAddress int64) (err error) {
	_, err = S.mapFile.Seek(bucketAddress+conf.BucketOverflowAddressOffset, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(overflowAddress))

	_, err = S.mapFile.Write(buf)
	if err != nil {
		return
	}

	return
}

// GetOverflowRecord - Gets a Record from the overflow file
func (S *SCFiles) GetOverflowRecord(recordAddress int64) (record model.Record, err error) {
	trueRecordLength := S.keyLength + S.valueLength + conf.InUseFlagBytes
	_, err = S.ovflFile.Seek(recordAddress, io.SeekStart)
	if err != nil {
		return
	}

	buf := make([]byte, trueRecordLength+conf.OverflowAddressLength)
	_, err = S.ovflFile.Read(buf)
	if err != nil {
		return
	}

	record, err = overflowBytesToRecord(buf, recordAddress, S.keyLength, S.valueLength)
	return
}

// SetOverflowRecord - Sets a Record in the overflow file
func (S *SCFiles) SetOverflowRecord(record model.Record) (err error) {
	buf := recordToOverflowBytes(record, S.keyLength, S.valueLength)

	_, err = S.ovflFile.Seek(record.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = S.ovflFile.Write(buf)

	return
}

// AppendOverflowRecord - Appends a record to the overflow file and updates the linking record with the new
// records address
func (S *SCFiles) AppendOverflowRecord(linkingRecord model.Record, key, value []byte) (err error) {
	overflowAddress, err := S.NewBucketOverflow(key, value)
	if err != nil {
		return
	}

	buf := make([]byte, conf.OverflowAddressLength)
	binary.LittleEndian.PutUint64(buf, uint64(overflowAddress))

	_, err = S.ovflFile.Seek(linkingRecord.RecordAddress, io.SeekStart)
	if err != nil {
		return
	}

	_, err = S.ovflFile.Write(buf)

	return
}

package lpres

import (
	"github.com/gostonefire/filehashmap/internal/model"
)

// bytesToBucket - Converts bucket raw data to a Bucket struct
func bytesToBucket(buf []byte, bucketAddress, keyLength, valueLength int64) (bucket model.Bucket, err error) {
	keyStart := stateBytes
	valueStart := keyStart + keyLength

	key := make([]byte, keyLength)
	value := make([]byte, valueLength)
	_ = copy(key, buf[keyStart:keyStart+keyLength])
	_ = copy(value, buf[valueStart:valueStart+valueLength])

	bucket = model.Bucket{
		Record: model.Record{
			State:         buf[0],
			RecordAddress: bucketAddress,
			Key:           key,
			Value:         value,
		},
		BucketAddress:   bucketAddress,
		OverflowAddress: 0,
		HasOverflow:     false,
	}

	return
}

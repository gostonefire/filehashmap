package crt

// NoRecordFound - Custom error to inform that no record was found
type NoRecordFound struct {
	msg string
}

// Error - Used to notify that no record was found
func (E NoRecordFound) Error() string {
	if E.msg == "" {
		return "no record found"
	}
	return E.msg
}

// MapFileFull - Custom error to inform that the map file is full and can't take more records
type MapFileFull struct {
	msg string
}

// Error - Used to notify that map file is full
func (E MapFileFull) Error() string {
	if E.msg == "" {
		return "no record found"
	}
	return E.msg
}

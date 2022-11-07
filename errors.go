package filehashmap

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

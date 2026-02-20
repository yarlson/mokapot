package sqs

import (
	"crypto/md5" //nolint:gosec // MD5 is required by the SQS protocol for message body checksums
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
)

var (
	ErrQueueDoesNotExist            = errors.New("AWS.SimpleQueueService.NonExistentQueue")
	ErrReceiptHandleIsInvalid       = errors.New("ReceiptHandleIsInvalid")
	ErrInvalidParameterValue        = errors.New("InvalidParameterValue")
	ErrEmptyBatchRequest            = errors.New("AWS.SimpleQueueService.EmptyBatchRequest")
	ErrTooManyEntriesInBatchRequest = errors.New("AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	ErrBatchEntryIdsNotDistinct     = errors.New("AWS.SimpleQueueService.BatchEntryIdsNotDistinct")
	ErrPurgeQueueInProgress         = errors.New("AWS.SimpleQueueService.PurgeQueueInProgress")
)

func md5Hash(s string) string {
	h := md5.Sum([]byte(s)) //nolint:gosec // required by SQS protocol
	return hex.EncodeToString(h[:])
}

// md5OfMessageAttributes computes the MD5 digest of message attributes using
// the AWS canonical encoding:
//   - Attributes sorted by name
//   - For each: 4-byte big-endian len + name, 4-byte big-endian len + data type,
//     1-byte transport type (1=String/Number, 2=Binary), 4-byte big-endian len + value
func md5OfMessageAttributes(attrs map[string]MessageAttribute) string {
	if len(attrs) == 0 {
		return ""
	}

	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names)

	h := md5.New() //nolint:gosec // required by SQS protocol
	buf := make([]byte, 4)

	for _, name := range names {
		attr := attrs[name]

		// Name
		binary.BigEndian.PutUint32(buf, uint32(len(name))) //nolint:gosec // attribute names are short
		h.Write(buf)
		h.Write([]byte(name))

		// Data type
		binary.BigEndian.PutUint32(buf, uint32(len(attr.DataType))) //nolint:gosec // data type strings are short
		h.Write(buf)
		h.Write([]byte(attr.DataType))

		// Transport type + value
		if strings.HasPrefix(attr.DataType, "Binary") {
			h.Write([]byte{2})
			binary.BigEndian.PutUint32(buf, uint32(len(attr.BinaryValue))) //nolint:gosec // values are bounded by SQS limits
			h.Write(buf)
			h.Write(attr.BinaryValue)
		} else {
			// String and Number types use string transport.
			h.Write([]byte{1})
			binary.BigEndian.PutUint32(buf, uint32(len(attr.StringValue))) //nolint:gosec // values are bounded by SQS limits
			h.Write(buf)
			h.Write([]byte(attr.StringValue))
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

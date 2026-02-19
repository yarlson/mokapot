package sqs

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
)

var (
	ErrQueueDoesNotExist      = errors.New("AWS.SimpleQueueService.NonExistentQueue")
	ErrReceiptHandleIsInvalid = errors.New("ReceiptHandleIsInvalid")
	ErrInvalidParameterValue  = errors.New("InvalidParameterValue")
)

func md5Hash(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

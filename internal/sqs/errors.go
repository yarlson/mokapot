package sqs

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
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
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

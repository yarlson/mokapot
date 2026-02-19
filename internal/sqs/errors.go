package sqs

import (
	"crypto/md5" //nolint:gosec // MD5 is required by the SQS protocol for message body checksums
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
	h := md5.Sum([]byte(s)) //nolint:gosec // required by SQS protocol
	return hex.EncodeToString(h[:])
}

package sns

import "errors"

var (
	ErrTopicNotFound         = errors.New("NotFound")
	ErrSubscriptionNotFound  = errors.New("SubscriptionNotFound")
	ErrInvalidParameter      = errors.New("InvalidParameter")
	ErrInvalidParameterValue = errors.New("InvalidParameterValue")
)

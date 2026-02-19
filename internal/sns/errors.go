package sns

import "errors"

var (
	ErrTopicNotFound         = errors.New("NotFound")
	ErrInvalidParameter      = errors.New("InvalidParameter")
	ErrInvalidParameterValue = errors.New("InvalidParameterValue")
)

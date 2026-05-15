package shortcut

import (
	"errors"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrValidation          = errors.New("validation error")
	ErrDuplicateKey        = errors.New("duplicate key")
	ErrForeignKeyViolation = errors.New("foreign key violation")
	ErrCheckViolation      = errors.New("check violation")
	ErrExclusionViolation  = errors.New("exclusion violation")
)

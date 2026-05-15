package dto

import (
	"time"
)

type Event struct {
	Time  time.Time `json:"time"`
	Email string    `json:"email"`
	Type  string    `json:"type"`
}

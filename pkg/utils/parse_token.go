package utils

import (
	"errors"
	"strings"
)

func ParseBearerToken(header string) (string, error) {
	if header == "" {
		return "", errors.New("authorization header is empty")
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return "", errors.New("invalid authorization header format")
	}

	if !strings.EqualFold(parts[0], "Bearer") {
		return "", errors.New("authorization type is not Bearer")
	}

	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", errors.New("token is empty")
	}

	return token, nil
}

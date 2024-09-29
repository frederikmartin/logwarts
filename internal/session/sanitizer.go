package session

import (
	"fmt"
	"regexp"
	"strings"
)

func SanitizeSessionName(sessionName string) (string, error) {
	sanitized := strings.ToLower(sessionName)

	re := regexp.MustCompile(`[^a-z0-9_]`)
	sanitized = re.ReplaceAllString(sanitized, "_")

	if len(sanitized) == 0 {
		return "", fmt.Errorf("Sanitized string is empty")
	} else {
		firstChar := sanitized[0]
		if (firstChar < 'a' || firstChar > 'z') && firstChar != '_' {
			sanitized = "_" + sanitized
		}
	}

	if len(sanitized) > 63 {
		sanitized = sanitized[:63]
	}

	return sanitized, nil
}

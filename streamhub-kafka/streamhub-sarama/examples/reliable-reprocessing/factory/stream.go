package factory

import "strings"

func NewStreamName(hostname, entity, action string) string {
	b := strings.Builder{}
	b.WriteString(hostname)
	b.WriteString(".")
	b.WriteString(entity)
	b.WriteString(".")
	b.WriteString(action)
	return b.String()
}

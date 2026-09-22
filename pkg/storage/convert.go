package storage

import "github.com/h3yow/audit-log-pipeline-go/pkg/event"

func eventFromJSON(b []byte) (*event.Event, error) {
	return event.ParseEvent(b)
}

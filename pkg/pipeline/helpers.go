package pipeline

import (
	"errors"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

func logEvent(e *event.Event) map[string]any {
	return map[string]any{
		"id":        e.ID,
		"source":    e.Source,
		"seq":       e.Seq,
		"type":      e.Type,
		"action":    e.Action,
		"timestamp": e.Timestamp.Format(time.RFC3339),
	}
}

func asSequenceError(err error, target **storage.SequenceError) bool {
	var se *storage.SequenceError
	if errors.As(err, &se) {
		*target = se
		return true
	}
	return false
}

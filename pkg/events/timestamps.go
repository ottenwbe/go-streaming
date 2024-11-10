package events

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"
)

type (
	StampMeta map[string]interface{}
	TimeStamp struct {
		StartTime time.Time `json:"start_time"`
		EndTime   time.Time `json:"end_time"`
		Meta      StampMeta `json:"meta"`
	}
)

func (t TimeStamp) Content() map[string]interface{} {
	result := map[string]interface{}{
		"start_time": t.StartTime,
		"end_time":   t.EndTime,
	}
	maps.Copy(result, t.Meta)
	return result
}

func (t TimeStamp) String() string {
	b, _ := json.Marshal(t)
	return fmt.Sprintf("%v", string(b))
}

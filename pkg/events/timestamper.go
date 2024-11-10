package events

import (
	"maps"
	"time"
)

func createStamp(meta StampMeta) TimeStamp {

	if meta == nil {
		meta = StampMeta{}
	}
	now := getTime()

	return TimeStamp{
		StartTime: now,
		EndTime:   now,
		Meta:      meta,
	}
}

func createStampBasedOnOthers(meta StampMeta, stamps ...TimeStamp) TimeStamp {
	if len(stamps) > 0 {
		minTime := stamps[0].StartTime
		maxTime := stamps[0].EndTime

		newMeta := StampMeta{}
		maps.Copy(newMeta, meta)

		for _, eventStamp := range stamps {

			if minTime.After(eventStamp.StartTime) {
				minTime = eventStamp.StartTime
			}

			if maxTime.Before(eventStamp.EndTime) {
				maxTime = eventStamp.EndTime
			}

			maps.Copy(newMeta, eventStamp.Meta)
		}

		return TimeStamp{
			StartTime: minTime,
			EndTime:   maxTime,
			Meta:      newMeta,
		}
	}
	return createStamp(meta)
}

func getTime() time.Time {
	return time.Now()
}

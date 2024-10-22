package events

import "time"

func GetTimeStamp() time.Time {
	return time.Now()
}

func MaxTimeStamp(t ...time.Time) time.Time {
	if len(t) > 0 {
		maxTime := t[0]
		for _, t2 := range t {
			if t2.After(maxTime) {
				maxTime = t2
			}
		}
		return maxTime
	}
	return GetTimeStamp()
}

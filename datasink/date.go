package datasink

import (
	"strconv"
	"time"
)

// ParseDateString: parse a bigquery partition format string (YYYYMMDD) into a
// time.Time
func ParseDateString(str string) (time.Time, error) {
	if len(str) != 8 {
		return time.Time{}, ErrInvalidDateStr{"must be 8 chars"}
	}

	year, err := strconv.Atoi(str[:4])
	if err != nil {
		return time.Time{}, ErrInvalidDateStr{"year: " + err.Error()}
	}

	month, err := strconv.Atoi(str[5:6])
	if err != nil {
		return time.Time{}, ErrInvalidDateStr{"month: " + err.Error()}
	}

	day, err := strconv.Atoi(str[6:8])
	if err != nil {
		return time.Time{}, ErrInvalidDateStr{"day: " + err.Error()}
	}

	if month < 0 || month > 12 {
		return time.Time{}, ErrInvalidDateStr{"invalid month"}
	}

	if day < 0 || day > 31 {
		return time.Time{}, ErrInvalidDateStr{"invalid day"}
	}

	ts := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

	if str == EarliestDate {
		return ts, nil
	}

	earliestTs, err := ParseDateString(EarliestDate)
	if earliestTs.After(ts) {
		return time.Time{}, ErrInvalidDateStr{"date must be after " + EarliestDate}
	}

	return ts, nil
}

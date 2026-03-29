package internal

import (
	"fmt"
	"strings"
)

// GenerateInsert returns a SQL INSERT statement for the given MeterReading.
// The statement uses ON CONFLICT to make repeated execution idempotent.
func GenerateInsert(r MeterReading) (string, error) {
	if strings.ContainsRune(r.NMI, '\'') {
		return "", fmt.Errorf("generator: NMI contains invalid character: %q", r.NMI)
	}

	ts := r.Timestamp.Format("2006-01-02 15:04:05")

	return fmt.Sprintf(
		"INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES ('%s', '%s', %s) "+
			"ON CONFLICT ON CONSTRAINT meter_readings_unique_consumption DO UPDATE SET consumption = EXCLUDED.consumption;",
		r.NMI, ts, r.Consumption,
	), nil
}

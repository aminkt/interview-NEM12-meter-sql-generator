package internal

import "time"

// MeterReading represents a single interval consumption reading for a meter.
type MeterReading struct {
	NMI       string
	Timestamp time.Time
	// Consumption is kept as a string to avoid IEEE 754 floating-point precision
	// loss. The database column is NUMERIC, so we pass the exact string value.
	Consumption string
}

package internal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

type NMIBlock struct {
	Header string
	Rows   []string
}

func SplitBlocks(ctx context.Context, r io.Reader) (<-chan NMIBlock, <-chan error) {
	out := make(chan NMIBlock, 64)
	errc := make(chan error, 16)

	go func() {
		defer close(out)
		defer close(errc)

		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

		var current *NMIBlock

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				errc <- fmt.Errorf("splitter: context cancelled: %w", ctx.Err())
				return
			default:
			}

			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			recordType := line
			if idx := strings.IndexByte(line, ','); idx > 0 {
				recordType = line[:idx]
			}

			switch recordType {
			case "100":
				// Header - skip.
				continue

			case "200":
				// Flush the previous block if any.
				if current != nil {
					select {
					case out <- *current:
					case <-ctx.Done():
						return
					}
				}
				current = &NMIBlock{Header: line}

			case "300":
				if current == nil {
					errc <- fmt.Errorf("splitter: 300 record before any 200 record")
					continue
				}
				current.Rows = append(current.Rows, line)

			case "500":
				// Flush the current block at the end of an NMI section.
				if current != nil {
					select {
					case out <- *current:
					case <-ctx.Done():
						return
					}
					current = nil
				}

			case "900":
				// End of file - flush any remaining block.
				if current != nil {
					select {
					case out <- *current:
					case <-ctx.Done():
						return
					}
					current = nil
				}
				return

			default:
				continue
			}
		}

		// Flush remaining block if file didn't end with 500/900.
		if current != nil {
			select {
			case out <- *current:
			case <-ctx.Done():
			}
		}

		if err := scanner.Err(); err != nil {
			errc <- fmt.Errorf("splitter: scanner error: %w", err)
		}
	}()

	return out, errc
}

func ParseBlock(block NMIBlock) ([]MeterReading, error) {
	headerFields := strings.Split(block.Header, ",")
	nmi, intervalMinutes, err := parseRecord200(headerFields, 0)
	if err != nil {
		return nil, fmt.Errorf("parseblock: %w", err)
	}

	var readings []MeterReading

	for i, row := range block.Rows {
		fields := strings.Split(row, ",")
		rowReadings, err := parseRecord300(fields, nmi, intervalMinutes, i+1)
		if err != nil {
			return nil, fmt.Errorf("parseblock [%s]: %w", nmi, err)
		}
		readings = append(readings, rowReadings...)
	}

	return readings, nil
}

// parseRecord200 extracts the NMI and interval length from a 200 record.
func parseRecord200(fields []string, lineNum int) (string, int, error) {
	if len(fields) < 9 {
		return "", 0, fmt.Errorf("parser: line %d: 200 record has %d fields, expected at least 9", lineNum, len(fields))
	}

	nmi := strings.TrimSpace(fields[1])
	if nmi == "" || len(nmi) > 10 {
		return "", 0, fmt.Errorf("parser: line %d: invalid NMI %q (must be 1-10 characters)", lineNum, nmi)
	}

	intervalLength, err := strconv.Atoi(strings.TrimSpace(fields[8]))
	if err != nil {
		return "", 0, fmt.Errorf("parser: line %d: invalid interval length %q: %w", lineNum, fields[8], err)
	}

	switch intervalLength {
	case 5, 15, 30:
		// Valid.
	default:
		return "", 0, fmt.Errorf("parser: line %d: unsupported interval length %d (expected 5, 15, or 30)", lineNum, intervalLength)
	}

	return nmi, intervalLength, nil
}

// parseRecord300 extracts all interval consumption readings from a 300 record.
func parseRecord300(fields []string, nmi string, intervalMinutes int, lineNum int) ([]MeterReading, error) {
	if len(fields) < 3 {
		return nil, fmt.Errorf("parser: line %d: 300 record has %d fields, expected at least 3", lineNum, len(fields))
	}

	intervalDate, err := time.Parse("20060102", strings.TrimSpace(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("parser: line %d: invalid interval date %q: %w", lineNum, fields[1], err)
	}

	intervalsPerDay := 1440 / intervalMinutes
	endIdx := 2 + intervalsPerDay

	if len(fields) < endIdx {
		return nil, fmt.Errorf("parser: line %d: 300 record has %d fields, expected at least %d for %d-min intervals",
			lineNum, len(fields), endIdx, intervalMinutes)
	}

	readings := make([]MeterReading, 0, intervalsPerDay)

	for i := 0; i < intervalsPerDay; i++ {
		raw := strings.TrimSpace(fields[2+i])
		if raw == "" {
			continue
		}

		if _, err := strconv.ParseFloat(raw, 64); err != nil {
			return nil, fmt.Errorf("parser: line %d: invalid consumption value at position %d: %q: %w",
				lineNum, i, raw, err)
		}

		ts := intervalDate.Add(time.Duration(i*intervalMinutes) * time.Minute)

		readings = append(readings, MeterReading{
			NMI:         nmi,
			Timestamp:   ts,
			Consumption: raw,
		})
	}

	return readings, nil
}

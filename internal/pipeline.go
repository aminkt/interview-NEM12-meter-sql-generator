package internal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
)

type Result struct {
	TotalReadings int
	Errors        int
}

func Run(ctx context.Context, input io.Reader, output io.Writer, workers int) (Result, error) {
	if workers < 1 {
		workers = 1
	}

	blocks, splitErrs := SplitBlocks(ctx, input)

	sqlCh := make(chan string, 512)
	var (
		wg          sync.WaitGroup
		mapErrors   atomic.Int64
		mapReadings atomic.Int64
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for block := range blocks {
				// Map: parse the entire NMI block (200 header + all 300 rows).
				readings, err := ParseBlock(block)
				if err != nil {
					log.Printf("map worker error: %v", err)
					mapErrors.Add(1)
					continue
				}

				// Map: generate SQL for each reading in the block.
				for _, r := range readings {
					sql, err := GenerateInsert(r)
					if err != nil {
						log.Printf("map worker generator error: %v", err)
						mapErrors.Add(1)
						continue
					}
					mapReadings.Add(1)

					select {
					case sqlCh <- sql:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	// Close the SQL channel once all map workers finish.
	go func() {
		wg.Wait()
		close(sqlCh)
	}()

	var (
		totalWritten int
		writeErr     error
	)
	bw := bufio.NewWriterSize(output, 64*1024)

	for sql := range sqlCh {
		if _, err := fmt.Fprintln(bw, sql); err != nil {
			writeErr = fmt.Errorf("reduce: write error: %w", err)
			break
		}
		totalWritten++
	}

	if err := bw.Flush(); err != nil && writeErr == nil {
		writeErr = fmt.Errorf("reduce: flush error: %w", err)
	}

	var splitErrCount int
	for err := range splitErrs {
		log.Printf("split warning: %v", err)
		splitErrCount++
	}

	totalErrors := splitErrCount + int(mapErrors.Load())

	if writeErr != nil {
		return Result{TotalReadings: totalWritten, Errors: totalErrors}, writeErr
	}

	return Result{TotalReadings: totalWritten, Errors: totalErrors}, nil
}

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/aminkt/interview-nem12-meter-sql-generator/internal"
)

func main() {
	inputPath := flag.String("input", "", "Path to the NEM12 input file (required)")
	outputPath := flag.String("output", "", "Path to the output SQL file (default: stdout)")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of parallel workers")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: nem12reader -input <path> [-output <path>] [-workers <N>]\n\n")
		fmt.Fprintf(os.Stderr, "Reads a NEM12 meter data file and generates SQL INSERT statements.\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *inputPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	inputFile, err := os.Open(*inputPath)
	if err != nil {
		log.Fatalf("failed to open input file: %v", err)
	}
	defer inputFile.Close()

	var output io.Writer = os.Stdout
	if *outputPath != "" {
		outFile, err := os.Create(*outputPath)
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer outFile.Close()
		output = outFile
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	start := time.Now()

	result, err := internal.Run(ctx, inputFile, output, *workers)
	if err != nil {
		log.Fatalf("pipeline error: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "Processed %d readings from %s in %dms (workers=%d, errors=%d)\n",
		result.TotalReadings, *inputPath, elapsed.Milliseconds(), *workers, result.Errors)
}

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	numNMIs := flag.Int("nmis", 100, "Number of unique NMIs to generate")
	numDays := flag.Int("days", 365, "Number of days of data per NMI")
	output := flag.String("output", "testdata/large.csv", "Output file path")
	intervalLen := flag.Int("interval", 30, "Interval length in minutes (5, 15, or 30)")
	seed := flag.Int64("seed", 42, "Random seed for reproducibility")

	flag.Parse()

	intervalsPerDay := 1440 / *intervalLen

	f, err := os.Create(*output)
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 256*1024)
	defer w.Flush()

	rng := rand.New(rand.NewSource(*seed))
	startDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	fmt.Fprintf(w, "100,NEM12,%s,TESTGEN,NEMMCO\n", time.Now().Format("200601021504"))

	totalRows := 0

	for nmiIdx := 0; nmiIdx < *numNMIs; nmiIdx++ {
		nmi := fmt.Sprintf("NEM%07d", nmiIdx+1)

		fmt.Fprintf(w, "200,%s,E1E2,1,E1,N1,%05d,kWh,%d,%s\n",
			nmi, nmiIdx+1, *intervalLen, startDate.Format("20060102"))

		for day := 0; day < *numDays; day++ {
			date := startDate.AddDate(0, 0, day)

			fmt.Fprintf(w, "300,%s", date.Format("20060102"))

			for i := 0; i < intervalsPerDay; i++ {
				hour := float64(i**intervalLen) / 60.0
				var base float64
				switch {
				case hour < 6:
					base = 0.1
				case hour < 9:
					base = 0.5 + (hour-6)*0.3
				case hour < 17:
					base = 1.0 + rng.Float64()*0.8
				case hour < 21:
					base = 0.8 + (21-hour)*0.1
				default:
					base = 0.2
				}
				val := base + rng.Float64()*0.3
				fmt.Fprintf(w, ",%.3f", val)
			}

			fmt.Fprintf(w, ",A,,,%s,\n", date.Add(24*time.Hour).Format("20060102150405"))
			totalRows++
		}

		fmt.Fprintf(w, "500,O,S%05d,%s,\n", nmiIdx+1, time.Now().Format("20060102150405"))
	}

	fmt.Fprintln(w, "900")

	if err := w.Flush(); err != nil {
		log.Fatalf("flush error: %v", err)
	}

	totalReadings := totalRows * intervalsPerDay
	stat, _ := f.Stat()
	sizeMB := float64(stat.Size()) / (1024 * 1024)

	fmt.Fprintf(os.Stderr, "Generated %s: %d NMIs × %d days = %d interval rows, %d total readings (%.1f MB)\n",
		*output, *numNMIs, *numDays, totalRows, totalReadings, sizeMB)
}

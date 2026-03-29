# Flo Energy Interview Task – NEM12 Meter Data SQL Generator

A production grade Go CLI tool that parses NEM12 formatted meter data files and generates SQL `INSERT` statements for the `meter_readings` table.
Designed for large-file streaming with concurrent processing via goroutines.

## Quick Start

```bash
# Build
make build

# Run with sample data (output to stdout)
make run

# Run with output to file
./bin/nem12reader -input testdata/sample.csv -output output.sql

# Run tests
make test
```

## Usage

```
Usage: nem12reader -input <path> [-output <path>] [-workers <N>]

Flags:
  -input    string   Path to the NEM12 input file (required)
  -output   string   Path to output SQL file (default: stdout)
  -workers  int      Number of parallel workers (default: NumCPU)
```

## Architecture

The application processes NEM12 files, which contain hierarchical data: a `200` record defines a meter (NMI), followed by multiple `300` records containing the actual interval consumption data based on that meter's configuration. To handle very large files efficiently without exhausting memory, the application implements a concurrent **Map-Reduce** pattern:

1. **Split (1 goroutine)**: Reads the NEM12 file stream line by line using. It parses the hierarchy, grouping a `200` record and all its subsequent `300` records into a single `NMIBlock` memory structure. This ensures constant memory usage regardless of the total file size. It then sends these blocks through a channel to the workers.
2. **Map (N goroutines)**: Multiple worker goroutines listen on the channel. Each worker receives an `NMIBlock`, parses the dates and interval values from the `300` records, and generates the corresponding SQL `INSERT` statements. Because each block is an independent unit of work, parsing and SQL generation are fully parallelized, drastically reducing processing time.
3. **Reduce (1 goroutine)**: A single writer goroutine collects the generated SQL strings from all map workers through a results channel and writes them sequentially to the output file (or stdout) using buffered I/O, preventing write contentions.

## Project Structure

```
  bin/                         # Compiled binary output
  cmd/nem12reader/main.go      # CLI entrypoint
  internal/
     model/model.go            # Domain types (MeterReading)
     parser/parser.go          # Streaming NEM12 parser
     generator/generator.go    # SQL INSERT statement generator
     pipeline/pipeline.go      # Concurrent processing pipeline
  testdata/sample.csv          # Sample NEM12 file
```

## Target Table

```sql
CREATE TABLE meter_readings (
    id            uuid DEFAULT gen_random_uuid() NOT NULL,
    nmi           varchar(10) NOT NULL,
    timestamp     timestamp NOT NULL,
    consumption   numeric NOT NULL,
    CONSTRAINT meter_readings_pk PRIMARY KEY (id),
    CONSTRAINT meter_readings_unique_consumption UNIQUE (nmi, timestamp)
);
```

## Output Format

Each reading produces an idempotent INSERT with upsert semantics:

```sql
INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES ('NEM1201009', '2005-03-01 06:00:00', 0.461)
ON CONFLICT ON CONSTRAINT meter_readings_unique_consumption DO UPDATE SET consumption = EXCLUDED.consumption;
```

---

## Write-Up

### Q1. What is the rationale for the technologies you have decided to use?

- **Go** was chosen specifically because of its lightweight concurrency primitives (**goroutines** and **channels**), which map naturally to a streaming parallelization pipeline. Unlike traditional OS threads, goroutines consume incredibly little overhead, allowing us to spin up many workers matching CPU cores. This ensures that large files are parsed much faster than sequence-based languages, satisfying the requirement to handle heavy workloads quickly.
- **Single Process/Binary** — The entire processing flow is contained within a single executable application, making deployment trivial with zero setup or external services required.
- **Standard library only** — The problem of streaming text files and parsing CSV-like fields is well-scoped and inherently fast using Go's `bufio` and `strings` modules. We avoid heavyweight libraries or ORMs (as permitted by the problem statement) to minimize memory usage. This allows a continuous stream from disk to processing to disk with constant memory regardless of file length.
- **PostgreSQL-compatible SQL output** — Matches the provided `CREATE TABLE` schema. The output statements use Upsert syntax (`ON CONFLICT... DO UPDATE`) to provide idempotent handling of re-run records.

### Q2. What would you have done differently if you had more time?

- Direct database insertion.
- `--batch-size` flag to wrap groups of INSERTs in explicit `BEGIN/COMMIT` transactions for better DB performance.
- Structured logging.

### Q3. What is the rationale for the design choices that you have made?

- **Map-Reduce pattern**: the NEM12 format naturally partitions into independent NMI blocks (each 200 record + its 300 records). Map-Reduce use this: the splitter groups lines into blocks, and N map workers process blocks fully in parallel (both parsing *and* SQL generation). This eliminates the single-threaded parser bottleneck of a simple fan-out pipeline.
- **Streaming splitter** : reads line-by-line with constant memory usage. Blocks are emitted as soon as they are complete, so the file is never fully loaded into memory. This is critical for handling very large files.
- **Channel-based stages**: cleanly decouples Split, Map, and Reduce. Each stage runs concurrently and communicates via typed Go channels, following idiomatic patterns.
- **Stateless Map function**: `ParseBlock()` is a pure function with no shared state; it can be called safely from any number of goroutines without synchronisation.
- **`ON CONFLICT DO UPDATE`**: makes the generated SQL idempotent. Re-running the output against the database updates existing rows rather than failing on duplicates.
- **Context propagation**: every stage respects `context.Context` for cancellation, enabling graceful shutdown on `SIGINT`/`SIGTERM`.
- **Single binary**: the entire tool compiles to one executable with no runtime dependencies, simplifying distribution and execution.



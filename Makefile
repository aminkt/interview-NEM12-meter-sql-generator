.PHONY: build test bench run run-large generate-large clean

build:
	go build -o bin/nem12reader ./cmd/nem12reader
	go build -o bin/testdatagen ./cmd/testdatagen

run: build
	./bin/nem12reader -input testdata/sample.csv

generate-large: build
	./bin/testdatagen -nmis 100 -days 365 -output testdata/large.csv

run-large: build generate-large
	./bin/nem12reader -input testdata/large.csv -output testdata/large_output.sql

clean:
	rm -rf bin/ testdata/large.csv testdata/large_output.sql



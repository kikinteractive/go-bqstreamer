.PHONY: deps test testintegration

.default: deps test

deps:
	go get -t

test:
	go test -coverprofile=coverage.txt

testintegration:
	# this requires environment variables to be set
	go test \
		-tags integration \
		-key $(BQSTREAMER_KEY) \
		-project $(BQSTREAMER_PROJECT) \
		-dataset $(BQSTREAMER_DATASET) \
		-table $(BQSTREAMER_TABLE) \
		--coverprofile=coverage.txt \
		./

clean:
	rm -f coverage.txt

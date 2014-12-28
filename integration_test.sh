#!/bin/sh
set -x
go test -v -tags=integration -key key.json -project PROJECT_NAME -dataset test -table test

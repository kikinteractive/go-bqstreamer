#!/bin/sh
set -x
go test -v -tags=integrationmulti -key key.json -project PROJECT_NAME -dataset test -table test

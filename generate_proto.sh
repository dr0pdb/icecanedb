#!/usr/bin/env bash

echo "generating go files for protobuff"

SRC_DIR=proto
DST_DIR=pkg

function generate() {
	file=$(basename $1)
	base_name=$(basename $1 ".proto")
	echo "generating for file: " $file
	mkdir -p $DST_DIR
	protoc -I=$SRC_DIR --go_out=$DST_DIR --go-grpc_out=$DST_DIR $SRC_DIR/$file
}

for file in `ls proto/*.proto`
    do
    generate $file
done

exit 0

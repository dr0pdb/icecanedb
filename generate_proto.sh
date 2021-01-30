#!/usr/bin/env bash

echo "generating go files for protobuff"

SRC_DIR=proto
DST_DIR=pkg/protogen

function generate() {
	file=$(basename $1)
	base_name=$(basename $1 ".proto")
	echo "generating for file: " $file
	mkdir $DST_DIR/$base_name
	protoc -I=$SRC_DIR --go_out=$DST_DIR/$base_name --go_opt=paths=source_relative $SRC_DIR/$file
}

for file in `ls proto/*.proto`
    do
    generate $file
done

exit 0

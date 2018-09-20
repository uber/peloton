#!/usr/bin/env bash
cd mimir-lib
VERSION=$(git rev-parse HEAD)
cd ..
rm -rf mimir-lib/.arcconfig mimir-lib/.arclint mimir-lib/.git mimir-lib/.gitignore \
	 mimir-lib/glide.yaml mimir-lib/glide.lock mimir-lib/Makefile mimir-lib/README.md
for file in $(find mimir-lib -type f); do
    echo "// @generated AUTO GENERATED - DO NOT EDIT! ${VERSION}" | cat - ${file} \
	 | sed 's|code.uber.internal/infra/mimir-lib|code.uber.internal/infra/peloton/mimir-lib|g' - > ${file}_copy
	mv ${file}_copy ${file}
done
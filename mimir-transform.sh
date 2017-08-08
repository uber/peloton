#!/usr/bin/env bash
for file in $(find mimir-lib -type f); do
    echo "// @generated AUTO GENERATED - DO NOT EDIT!" | cat - ${file} \
	 | sed 's|code.uber.internal/infra/mimir-lib|code.uber.internal/infra/peloton/mimir-lib|g' - > ${file}_copy
	mv ${file}_copy ${file}
done

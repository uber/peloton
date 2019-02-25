#!/bin/bash


MIMIR_REPO=$1
MIMIR_HOME=placement/plugins/mimir/lib
FILES_TO_REMOVE="
	.arcconfig
	.arclint
	.git
	.gitignore
    	glide.yaml
    	glide.lock
    	Makefile
    	README.md
"
    
if [ -z $MIMIR_REPO ]
then
    echo "Usage: $(basename $0) mimir_git_repo"
    exit
fi


rm -rf $MIMIR_HOME
git clone $MIMIR_REPO $MIMIR_HOME

# Find the current mimir version
pushd .
cd $MIMIR_HOME
VERSION=$(git rev-parse HEAD)
popd

# Delete the files that are no longer needed
for file in $FILES_TO_REMOVE; do
    echo $file
    rm -rf $MIMIR_HOME/$file
done

# Replace the golang package name
for file in $(find $MIMIR_HOME -type f); do
    # Prepend the auto generated comment
    echo "// @generated AUTO GENERATED - DO NOT EDIT! ${VERSION}

$(cat $file)" > ${file}

    # Replace the golang package name
    sed 's|code.uber.internal/infra/mimir-lib|github.com/uber/peloton/placement/plugins/mimir/lib|g' ${file} > $file.tmp && mv ${file}.tmp ${file}
done

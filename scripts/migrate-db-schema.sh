#!/bin/bash

set -ex


main() {
    host="localhost:9042"
    db="peloton_test"
    path="pkg/storage/cassandra/migrations"
    option=""

    while [ $# -gt 0 ] ; do
        case "$1" in
            "--host" )
                host="$2"
                shift 2 ;;
            "--db" )
                db="$2"
                shift 2 ;;
            "--username" )
                username="$2"
                shift 2 ;;
            "--password" )
                password="$2"
                shift 2 ;;
            "--path" )
                path="$2"
                shift 2 ;;
            "--option" )
                option="$2"
                shift 2 ;;
        esac
    done

    # Set username & password if specified
    [ ! -z $username ] && [ ! -z $password ] && host="${username}:${password}@${host}"

    [ -z $option ] && echo "Option missing: please specify up / down" && exit 1

    cat << EOF
    Please confirm the migration info :
    host : $host
    db   : $db
    migration file path : $path
    migration option : $option
EOF

    read -r -p "Are you sure you want to proceed? [y/N] " response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])+$ ]]
    then
        $GOPATH/bin/migrate --url "cassandra://${host}/${db}?protocol=4&consistency=all&disable_init_host_lookup" \
        --path ${path} \
        ${option} \
        > >(tee -a stdout.log) 2> >(tee -a stderr.log >&2)
    else
        echo 'Thank you, bye!'
    fi
}


usage() {
    echo "usage: $0 [--option MIGRATION_OPTION (up or down)]"
    echo "[--host CASSANDRA_HOST_PORT] [--db CASSANDRA_DB_NAME]"
    echo "[--username USER] [--password PASSWORD]"
    echo "[--path PATH_TO_MIGRATION_FILES]"
}


if [ $# -eq 0 ] ; then
    usage
    exit 1
fi

case "$1" in
    -h|--help ) usage ;;
    * ) main "$@" ;;
esac

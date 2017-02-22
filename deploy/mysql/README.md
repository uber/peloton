We use release-mysql.sh to build a aurora friendly percona mysql docker image and publish to uber/atg
docker registries. The docker related files are downloaded from percona repo.

Usage:

1. Release to ATG docker registry
From laptop run below command:
 ./release-mysql.sh --atg 1 --tag <tag of your choise, use 'latest' for offical release>

2. Release to Uber docker registry
scp the whole "mysql" dir to any ubuild machine and run below command from ubuild host:
 ./release-mysql.sh --prod 1 --tag <tag of your choise, use 'latest' for offical release>

3. Build locally
 ./release-mysql.sh

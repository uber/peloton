Peloton Dev
===========

-   [Project Page](http://t.uber.com/peloton)
-   [Runbook](https://code.uberinternal.com/w/runbooks/peloton/)

Install
=======

Installations of protoc/proto/protoc-gen-go are required, run
bootstrap.sh once so all build dependencies will be installed. Want to
build debian package or docker image ? Follow packaging/README.md

\$ cd \$GOPATH

\$ mkdir -p src/github.com/uber/

\$ git clone <gitolite@code.uber.internal>:infra/peloton
src/github.com/uber/peloton

\$ cd \$GOPATH/src/github.com/uber/peloton

\$ ./bootstrap.sh

\$ glide install

\$ make devtools

\$ make

Run Peloton apps in containers
==============================

Build docker image:

    $ IMAGE=uber/peloton make docker

Launch all dependencies and peloton apps in containers:

    $ PELOTON=app make pcluster

Test Peloton
============

Create resource pool by providing the path(respool is required for job
creation):

    $ bin/peloton respool create /DefaultResPool example/default_respool.yaml
    Resource Pool d214ed86-1cf5-4e39-a0bb-08399ab1dee0 created at /DefaultResPool

Create job:

    $ bin/peloton job create /DefaultResPool example/testjob.yaml
    Job 91b1b8e5-2ba8-11e7-bc23-0242ac11000d created

Get tasks:

    $ bin/peloton task list <job ID>

Run unit tests
==============

Run unit tests:

    $ make test

Run integration tests
=====================

Against local pcluster:

    $ make integ-test

Against a real cluster: irn1 (ATG VPN connection required):

    $ CLUSTER=<name of the cluster, i.e. peloton-devel01> make integ-test

dca1/sjc1 (run this from any compute host in the target dc):

    $ CLUSTER=<name of the cluster, i.e. dca1-devel01> make integ-test

Run peloton from docker container
---------------------------------

Build
=====

Build:

    $ make docker
    ...
    Built uber/peloton:51f1c4f

If you want to build an image with a different name: [IMAGE=foo/bar:baz
make docker]{.title-ref}

Run
===

The docker container takes a few environment variables to configure how
it will run. Each peloton app is launchable by setting
[APP=\$name]{.title-ref} in the environment. For example, to run the
peloton-master, execute:

    $ docker run --rm --name peloton -it -p 5289:5289 -e APP=master -e ENVIRONMENT=development peloton

Configurations are stored in [/etc/peloton/\$APP/]{.title-ref}, and by
default we will pass the following arguments: [-c
\"/etc/peloton/\${APP}/base.yaml\" -c
\"/etc/peloton/\${APP}/\${ENVIRONMENT}.yaml\"]{.title-ref}

NOTE: you need to make sure the container has access to all the
dependencies it needs, like mesos-master, zookeeper, cassandra, etc.
Check your configs!

Master with pcluster
====================

Master with pcluster:

    $ make pcluster
    $ docker run --rm --name peloton -it -e APP=master -e ENVIRONMENT=development --link peloton-mesos-master:mesos-master --link peloton-zk:zookeeper --link peloton-cassandra:cassandra peloton

Client
======

Launching the client is similar (replace [-m]{.title-ref} argument with
whereever your peloton-master runs:

    $ docker run --rm -it --link peloton:peloton -e APP=client peloton job -m http://peloton:5289/ create test1 test/testjob.yaml

Packaging
=========

Build debs for supported distributions. Output will be placed into
[./debs]{.title-ref}. You can specify the DISTRIBUTION by passing
[DISTRIBUTION=jessie]{.title-ref} (jessie and trusty are supported).
Defaults to [all]{.title-ref}.:

    $ make debs

Tagging a new release
=====================

Releases are managed by git tags, using semantic versioning. To tag a
new release:

Check the current version:

    $ make version
    0.1.0-abcdef

Make sure you are on master, and have the proper sha at HEAD you want to
tag. Then, increment the version and tag, then push tags:

    $ git tag -a 0.2.0
    ...
    $ git push origin --tags

Pushing docker containers
=========================

[make docker-push]{.title-ref} will build docker containers, and push
them to both ATG and SJC1 registries. You can push to only one DC with
[DC=atg]{.title-ref} or [DC=sjc1]{.title-ref}. You can override the
image to push with [IMAGE=foo/bar:baz]{.title-ref}

To build and deploy docker containers everywhere:

    make docker docker-push

Debug Peloton Apps in Docker Container
--------------------------------------

1\. Find docker container process ID: sudo docker inspect -f
{{.State.Pid}} \<DOCKER\_IMAGE\_ID\>

2\. Run a bash shell in the container: nsenter -t \<PID\> -m -p bash

3\. Setup source code directory symlink: mkdir -p
/workspace/src/github.com/uber/ ln -s /peloton-install
/workspace/src/\$(make project-name)

4\. Start the gdb in the bash shell: gdb
peloton-install/bin/peloton-\[hostmgr\|resmgr\|jobmgr\|placement\] \<PID\>

5.  Happy debugging ;-)

Pressure test the cassandra store
---------------------------------

We have a tool for pressure testing the cassandra store, which is based
on the storage.TaskStore interface.

1\. Build the cassandra store tool: make db-pressure

2.  Run test against a cassandra store. For example

bin/dbpressure -s peloton\_pressure -t 1000 -w 200 -h
ms-3162c292.pit-irn-1.uberatc.net -c ONE

Also need to make sure to have the schema migration files under
storage/cassandra/migrations

After running the load into C\*, one can check the C\* dashboard, for
example
<https://graphite.uberinternal.com/grafana2/dashboard/db/cassandra-mesos-irn>

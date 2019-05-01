# Developing Peloton

This document is intended for the developers who want to contribute to
the Peloton project. A typical development lifecyle for Peloton is
something like:

- Check out the Peloton code
- Make some code changes
- Run all test suites (unit, integration, failure, performance)
- Package and release a new version
- Deploy to canary and production clusters

## Setup Environment
We need to setup certain tools in order to build Peloton.

* For MacOS:
    - Install [Homebrew](https://brew.sh/) or update brew:

	```
      $ brew update
	```

    - Install Command Line Tools via [xcode-select](http://osxdaily.com/2014/02/12/install-command-line-tools-mac-os-x/)
    ```
	  $ xcode-select --install
    ```
    - Install golang (version 1.10.2+ is required) and glide

    ```
	  $ brew install golang
	  $ brew install glide
	```

* For Linux:
     - Intall golang (version 1.10.2+ is required) ([instructions](https://golang.org/doc/install#install))

* Setup GOPATH ([instructions](https://github.com/golang/go/wiki/SettingGOPATH))

```
$ export GOPATH=$HOME/go-workspace
$ export PATH=$PATH:$GOPATH/bin
```

## Checkout code
The Peloton repo has to be checked out to the specific source directory
`github.com/uber` under a predefined `$GOPATH`.

```
$ cd $GOPATH
$ mkdir -p src/github.com/uber/
$ git clone https://github.com/uber/peloton.git src/github.com/uber/peloton
```

## Setup environment
To install all build dependencies required by Peloton, we need to run the bootstrap steps as follows:
```
$ cd $GOPATH/src/github.com/uber/peloton
$ ./scripts/bootstrap.sh
$ glide install
$ make devtools
```

## Build

### Build Peloton binaries
```
$ make
```

### Build Peloton docker image

```
$ IMAGE=uber/peloton make docker
Built uber/peloton:51f1c4f
```

To build an image with a different name, run:
```
$ IMAGE=foo/bar:baz make docker
```


## Run Peloton

### Start a Peloton minicluster
A `minicluster` can be started locally on a development machine. It
consists of Peloton components as well as dependencies like Zookeeper,
Cassandra, Mesos master and agents. A minicluster launches all
components via docker engine which has to be installed first. Below
are the steps to start a minicluster:

1. Install docker engine (>=1.12.1) per [instruction here](https://docs.docker.com/v17.12/install/)

2. Start minicluster with local built Peloton image

```
$ PELOTON=app make minicluster
```


### Run each Peloton component

The docker container takes a few environment variables to configure
how it will run. Each peloton app is launchable by setting `APP=$name`
in the environment. For example, run peloton-jobmgr as follows:

```
$ docker run --rm --name peloton --net=host -it -p 5289:5289 -e APP=jobmgr -e ENVIRONMENT=development uber/peloton
```


Configurations are stored in `/etc/peloton/$APP/`, and by default we
will pass the following arguments: `-c "/etc/peloton/${APP}/base.yaml"
-c "/etc/peloton/${APP}/${ENVIRONMENT}.yaml"`

NOTE: make sure the container has access to all dependencies such as
mesos-master, zookeeper, cassandra, etc.


## Use Peloton

### Create a resource pool:

Creating a resource pool requires a resource pool path and the
resource pool spec.

```
$ bin/peloton respool create /DefaultResPool example/default_respool.yaml
Resource Pool d214ed86-1cf5-4e39-a0bb-08399ab1dee0 created at /DefaultResPool
```

### Create a job:

Creating a job requires a resource pool and the job spec.

```
$ bin/peloton job create /DefaultResPool example/testjob.yaml
Job 91b1b8e5-2ba8-11e7-bc23-0242ac11000d created
```

### Get the job status:


###  List all tasks in a job:

```
$ bin/peloton task list <job ID>
Instance|        Job|  CPU Limit|  Mem Limit|  Disk Limit|      State|  GoalState|  Started At|                                                                       Task ID|  Host|  Message|  Reason|
         0|  instance0|        0.1|       2 MB|       10 MB|  SUCCEEDED|  SUCCEEDED|       <nil>|   91b1b8e5-2ba8-11e7-bc23-0242ac11000d-0-91b7fa56-2ba8-11e7-bc23-0242ac11000d|      |         |        |
         1|  instance1|        0.1|       2 MB|       10 MB|  SUCCEEDED|  SUCCEEDED|       <nil>|   91b1b8e5-2ba8-11e7-bc23-0242ac11000d-1-91b82b47-2ba8-11e7-bc23-0242ac11000d|      |         |        |
         2|  instance2|        0.1|       2 MB|       10 MB|  SUCCEEDED|  SUCCEEDED|       <nil>|   91b1b8e5-2ba8-11e7-bc23-0242ac11000d-2-91b837b9-2ba8-11e7-bc23
```

## Testing

The testing suites of Peloton can be categorized into four groups:
- unit tests
- integration tests
- failure tests
- performance tests


### Unit tests

```
$ make test
```

### Integration tests

- Against local minicluster:
```
$ make integ-test
```

- Against a real cluster:
```
$ CLUSTER=<cluster-name> make integ-test
```

### Failure tests
- Against local minicluster:
```
$ make failure-test-minicluster
```
- Aginst a virtual cluster:
```
$ make failure-test-vcluster
```

### Performance tests
```
TBD
```

## Packaging

### Build Peloton deb package:
Build debs for supported distributions. Output will be placed into
`./debs`. You can specify the distribution by passing
`DISTRIBUTION=jessie` (jessie and trusty are supported). Defaults to
`all`.

```
$ make debs
```

### Build Peloton docker image:
Build docker image for Peloton and push the image to a docker registry:

```
$ make docker docker-push
```

## Release a new version

Releases are managed by git tags, using semantic versioning. To tag a new release:

Check the current version:
```
$ make version
0.1.0-abcdef
```

Make sure you are on master, and have the proper sha at HEAD you want to tag. Then,
increment the version and tag, then push tags:

```
$ git tag -a 0.2.0
...
$ git push origin --tags
```

## Debugging

### Debugging in docker container

1. Find docker container process ID:
```
$ docker ps
$ docker inspect <DOCKER_CONTAINER_ID>
```

2. Run a bash shell in the container:
```
$ nsenter -t <PID> -m -p bash
```

3. Setup source code directory symlink:
```
$ mkdir -p /workspace/src/github.com/uber/
$ ln -s /peloton-install /workspace/src/$(make project-name)
```

4. Start the gdb in the bash shell:
```
$ gdb peloton-install/bin/peloton-[hostmgr|jobmgr|resmgr|placement] <PID>
```

## Style, format, linting

### Go formatting

We use the standard `go fmt` tool to format the Go source code. All Go code must be run
through this auto formatter before being merged.

### Python formatting

We use autopep8 (which should be installed by default once you
setup your virtualenv) to format our python files. The configuration
for this formatter is at /.pep8 from the repo root.

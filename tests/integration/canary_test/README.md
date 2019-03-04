Canary test framework provides an interface to test stateless jobs in long
running staging environment

Features
- Framework makes sure before tests trigger, all jobs are in desired state.
- Randomly matches test and job.
- Configurable count to run a test multiple times (count <= #jobs), and
  each test run will pick random job.
- Run multiple tests in parallel (parallelism <= #jobs).
- Test run sequence is random.
- After test run completes, either success or failure job is restored
  to original state.


Run canary test locally
  - GOOS=linux GOARCH=amd64 BIN_DIR=bin-linux make
  - BIND_MOUNTS=$(pwd)/bin-linux:/go/src/github.com/uber/peloton/bin JOB_TYPE=SERVICE PELOTON=app make minicluster
  - CLUSTER=local pytest -vrsx -n=<#jobs> --count=<#jobs> --random-order-seed=<> tests/integration/canary_test

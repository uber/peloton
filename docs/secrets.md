# Secrets Management

This section describes distributing containers secrets at runtime via Peloton

## Background

Spark workloads can be run on Peloton in a similar way as they are run on YARN.
Spark workloads that access secure HDFS clusters to run data processing on
secure PII data, do so using HDFS delegation tokens. Distributing secrets
securely on cluster schedulers is inherently a difficult task. YARN ecosystem
achieves this using Kerberos delegation tokens. YARN has a mechanism to
authenticate users, and pass along their delegation tokens to YARN application
manager. Kerberos is prevalent in Hadoop ecosystem but not widely supported
at Uber. In order to securely pass delegation tokens to Spark containers,
Peloton has devised a generic secrets distribution mechanism using Mesos secrets

Although the design is geared towards the Spark on Peloton use case, the feature
is generic and can be used to pass along any secrets (credentials, JWTs, AWS
access keys etc.) to the containers running on Peloton

## Architecture

Peloton uses Mesos volume/secret isolator to pass secrets from Peloton to
running containers via Mesos Master and Mesos slave. Peloton accepts a list
of secrets (base64 encoded data and container mount path) per job as part of
Job.Create() and Job.Update() API. Peloton securely passes along these secrets
via Mesos to the containers at runtime.

The secret proto message to describe job secrets looks like this:
```
    message Secret {
      message Value {
        // Secret data as byte array
        bytes data = 1;
      }
      // UUID of the secret
      SecretID id = 1;
      // Path at which the secret file will be mounted in the container
      string path = 2;
      // Secret value
      Value value = 3;
    }
```

Job Create/Update/Get API will support this secret proto message.
For further info, see the complete [API documentation](api-reference.md)

### Spark on Peloton workflow

Spark containers specifically use this as described below:

![image](figures/secrets-workflow.png)

1.  Spark driver task is created on Peloton. At the time of creation, a
    delegation token is provided as a secret proto message to Peloton.
    The message contains the base64 encoded secret blob as well as the secret
    path (path at which the spark container will expect to read the secret).
    Multiple secrets can be provided during job creation.

2.  Peloton jobmgr creates a secret-id for each secret in job create request
    and stores it securely in Cassandra. Jobmgr adds a default task config to
    this job that contains the volume/secret container config which references
    the secrets by secret-ids

3.  Internally, default config for the job will have a container config like 
    this to describe secrets:
    ```
        "container":{
          "type": "MESOS",
          "volumes": [{
              "mode": "RO",
              "container_path": "/tmp/secret1",
              "Source": {
                  "type": "SECRET",
                  "secret": {
                      "type": "VALUE",
                      "value": {
                          "data": <secret id>,
                      }
                  }
              }
          }]
    ```

4.  At the time of task launch, Jobmgr will check if task config has a 
    volume/secret. If it finds a volume/secret config, it will replace the
    <secret-id> for each secret with secret data stored on Cassandra, and then
    launch the spark-driver task. Mesos agent on which this task has been
    scheduled will have a secrets isolator that will download the secret file
    locally on host at a tmpfs location

5.  Mesos isolator will then create a ramfs mount on the spark container and
    move the secret file to the location specified in Job Create API

6.  Spark-driver container will be able to read the delegation token from the
    mount path. It can then call Job.Update on its job ID along with the list of
    secrets, to add Spark-executor containers and pass on the delegation tokens
    to these new containers.

7.  Job Update will result in launching spark executor containers which then
    have access to the job secrets (delegation tokens) mounted at the path
    specified in Job Update API.

8.  Spark executor can now access secure HDFS tables using the delegation token

## Future work

Peloton team is planning to add secrets as first class citizens with a CRUD API
in subsequent releases. We are also planning to support secret store plugins
like Vault to download secrets by reference on runtime.

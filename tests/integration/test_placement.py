from job import IntegrationTestConfig, Job


def test__create_a_batch_job_with_unique_task_requirements():
    job = Job(job_file='test_job_no_container.yaml',
              config=IntegrationTestConfig(max_retry_attempts=150))
    # This will emulate a real production environment with many different tasks
    # running concurrently.
    job.job_config.instanceCount = 25
    job.create_per_task_configs()
    for i in range(job.job_config.instanceCount):
        job.job_config.instanceConfig[i].name = 'task-%s' % i
        job.job_config.instanceConfig[i].resource.memLimitMb = (
            24.0 + float(i / 5))
        job.job_config.instanceConfig[i].resource.diskLimitMb = (
            24.0 + float(i % 5))
    job.create()
    job.wait_for_state()

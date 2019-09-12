make batch-integ-test

# Skipping building Docker image because it should have
# been built by previous test
SKIP_BUILD=1 make hostpool-integ-test

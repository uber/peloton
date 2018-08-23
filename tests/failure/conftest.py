import pytest

import failure_fixture


@pytest.fixture(scope="function")
def failure_tester():
    """
    Creates the test fixture to use for failure tests.
    """
    fix = failure_fixture.FailureFixture()
    print       # to format log output
    fix.setup()
    yield fix

    fix.teardown()

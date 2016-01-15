from __future__ import absolute_import

import os
import sys
import shutil

from fabric.api import task, local, settings
from fabric.colors import green, red
from fabric.context_managers import prefix
from functools import wraps

# Path to directory holding virtualenvs.
WORKON_HOME = os.environ.get('WORKON_HOME', os.path.expanduser('~/.virtualenvs'))
# Paths to try when looking for env/bin/activate.
ACTIVATE_PATHS = [
    os.path.join(WORKON_HOME, 'peloton', 'bin', 'activate'),
]


def run_in_virtualenv(func):
    """Make sure the task is run in virtualenv (decorator)."""

    @wraps(func)
    def wrapper(*args, **kwargs):

        if os.environ.get('VIRTUAL_ENV'):
            # Already in virtualenv, no need to wrap the function.
            return func(*args, **kwargs)

        # Not in a virtualenv. Search for activate scripts.
        for activate in ACTIVATE_PATHS:
            print activate
            if os.path.isfile(activate):
                with prefix('source %s' % activate):
                    return func(*args, **kwargs)

        raise EnvironmentError('Unable to find a python virtualenv!')

    return wrapper


@task
def clean():
    """Remove all .pyc files."""
    print green('Clean up .pyc files')
    local('find . -name "*.py[co]" -exec rm -f "{}" ";"')
    print green('Removing unit test log')
    local('rm -f .unit_tests.log')
    dirs_to_clean = ['cover', '.cache']
    for path in dirs_to_clean:
        if os.path.isdir(path):
            print green('Clean up %s' % path)
            shutil.rmtree(path, ignore_errors=True)


@task
@run_in_virtualenv
def flake8():
    """Check for lints"""
    print green('Checking for lints')
    local('flake8 peloton tests')


@task
@run_in_virtualenv
def bootstrap(env='development'):
    """Bootstrap the environment."""
    print green('\nInstalling requirements')
    local('pip install -r requirements.txt')
    # For some envs, install requirements for unit-testing.
    if env in ['development', 'local', 'test']:
        local('pip install -r requirements-test.txt')
    local('python setup.py develop')
    os.system('mkdir -p /var/log/uber/peloton')


@task
@run_in_virtualenv
def test(args="", cov_report='term-missing', junit_xml=None, env='test'):
    """Run the test suite."""
    clean()
    setenv(env=env)

    if cov_report:
        args = '%s --cov-config .coveragerc --cov-report %s --cov peloton' % (
            args,
            cov_report,
        )

    if junit_xml:
        args = '%s --junit-xml=%s' % (args, junit_xml)

    cmd = 'py.test --tb=short %s peloton' % (args, )

    with settings(warn_only=True):
        success = local(cmd).succeeded

    status = "success" if success else "errors"
    color = green if success else red
    print color("Tests finished running with %s." % status)

    if not success:
        sys.exit(1)


@task
def coverage():
    """Run the coverage."""
    test(cov_report='html')
    local('open cover/index.html')


@task
@run_in_virtualenv
def jenkins(env='test'):
    """Bootstrap and run tests for continuous integration."""
    bootstrap(env)
    flake8()
    clean()
    test(cov_report='xml', junit_xml='jenkins.xml', env=env)


@task
@run_in_virtualenv
def serve_agent(env='development'):
    """Start the peloton agent."""
    setenv(env=env)
    local('peloton-agent')

    
@task
@run_in_virtualenv
def serve_master(env='development'):
    """Start the peloton master."""
    setenv(env=env)
    local('peloton-master')

    
def setenv(env='development'):
    """Set clay environment"""
    os.environ['CLAY_CONFIG'] = './config/%s.yaml' % env


@task
@run_in_virtualenv
def idl_gen():
    """Generate stubs from thrift idl"""
    idl_path = '../idl/code.uber.internal/infra/peloton'
    files = os.listdir(idl_path)
    thrift_files = [f for f in files if f.endswith('.thrift')]
    args = '-out gen --gen py:tornado,new_style,dynamic,slots,utf8strings'
    for thrift_file in thrift_files:
        print "Compiling %s ..." % thrift_file
        local('thrift %s %s/%s' % (args, idl_path, thrift_file))

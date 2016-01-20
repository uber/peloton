from tornado import gen, httpclient
from tornado.ioloop import IOLoop

from gen.peloton.goal_state import (
    GoalStateManager, GoalState, StateKey, StateQuery, StateAlreadyExists
)
from peloton.rpc.client import ThriftHttpClient


@gen.coroutine
def test_goal_state():
    client = ThriftHttpClient('localhost', 5289)
    client.add_service(GoalStateManager, '/goal_state/goal_state_manager')
    states = [
        GoalState(
            StateKey('config_bundle', 'homer-kafka-ingester', 1),
            '2016-01-01T00:00:00',
            'opaque string',
        )
    ]

    # set goal states twice and expect alread exists exception
    for i in xrange(2):
        try:
            yield client.invoke(GoalStateManager, 'setGoalStates', states=states)
        except StateAlreadyExists as e:
            print 'Ignore exception: %s' % e

    # query states
    queries = [
        StateQuery('config_bundle', 'homer-kafka-ingester', 0)
    ]
    result = yield client.invoke(GoalStateManager, 'queryGoalStates', queries=queries)
    print 'Query states return: \n%s' % result


if __name__ == '__main__':
    try:
        IOLoop.current().run_sync(test_goal_state)
    except httpclient.HTTPError as e:
        print 'Got HTTP error: %s' % e.response.body
        raise

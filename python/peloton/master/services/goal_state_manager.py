from __future__ import absolute_import

from clay import config
from tornado import gen

from peloton.rpc import thrift_service
from gen.peloton.goal_state import GoalStateManager, StateAlreadyExists

log = config.get_logger(__name__)


@thrift_service('/goal_state/goal_state_manager')
class GoalStateManagerImpl(object):

    __thrift_service__ = GoalStateManager

    def __init__(self):
        self.goal_state_map = {}

    @gen.coroutine
    def setGoalStates(self, states):
        log.info('GoalStateManager: set %d goal states' % len(states))

        existing_keys = []
        for state in states:
            key = ':'.join(
                [state.key.moduleName, state.key.instanceId, str(state.key.version)]
            )
            if key in self.goal_state_map:
                existing_keys.append(state.key)
            else:
                self.goal_state_map[key] = state

        if existing_keys:
            raise StateAlreadyExists(existingStates=existing_keys)

    @gen.coroutine
    def queryGoalStates(self, queries):
        log.info('GoalStateManager: query %d goal states' % len(queries))

        # Return all states for testing
        raise gen.Return(self.goal_state_map.values())

    @gen.coroutine
    def updateActualStates(self, states):
        log.info('GoalStateManager: update %d actual states' % len(states))

    @gen.coroutine
    def queryActualStates(self, queries):
        log.info('GoalStateManager: query %d actual states' % len(queries))


goal_state_manager = GoalStateManagerImpl()

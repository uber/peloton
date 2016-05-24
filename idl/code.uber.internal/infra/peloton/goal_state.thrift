/**
 *  Goal state Thrift interface
 */

namespace java com.uber.infra.peloton
namespace py peloton

/** DateTime in ISO format */
typedef string DateTime


/**
 *  Composite key of a goal state or actual state. Unique to identify
 *  a state for a module at a specific version.
 */
struct StateKey
{
  /** Name of the module which handles the goal state */
  1: required string moduleName;

  /**
   *  Identity of the instance where a goal state will be applied.
   *  For example, this could be the serivce instance key in UNS format.
   */
  2: required string instanceId;

  /**
   *  Version number of the goal state which is monotonically increasing.
   *  Goal state clients and executors can use this to guide against
   *  race conditions using MVCC.
   */
  3: required i64 version;
}


/**
 *  Generic goal state definitation for each instance and module. 
 *  The goal state represents the desired state of a module for a service
 *  instance in a Peloton/Mesos cluster. It is typically set by high-level 
 *  applications like uDeploy upgrade workflow, and will be synced to hosts
 *  and applied by the specific module via goal state agents.
 */
struct GoalState
{
  /** The identity of the goal state */
  1: required StateKey key;

  /** The timestamp when the goal state is updated */
  2: required DateTime updatedAt;

  /** The opaque state data that will be handled by the module */
  3: optional string data;
}


/**
 *  Generic actual state definitation for each instance and module.
 *  The actual state represents the actual state of a module for a service
 *  instance running in a Peloton/Mesos cluster. It is reported by goal
 *  state agents to the master via periodical updates. If there is no recent
 *  actual state change, only state key and updatedAt but not data will be
 *  present in the request. In this case, the actual state update can be treated
 *  as a keep alive update.
 */
struct ActualState
{
  /** The key of the actual state */
  1: required StateKey key;
  
  /** The timestamp when the actual state is updated */
  2: required DateTime updatedAt;
  
  /** The opaque state data that is reported by the module */
  3: optional string data;
}


/**
 *  Query for goal or actual states that match the list of attributes such as
 *  moduleName, instanceId and minVersion
 */ 
struct StateQuery
{
  /** Name of the module which handles the goal state */
  1: required string moduleName;

  /**
   *  Identity of the instance where a goal state will be applied.
   *  For example, this could be the serivce instance key in UNS format.
   */
  2: required string instanceId;

  /**
   *  Minimum version number on which the goal or actual states should be
   *  greater than or equal to.
   */
  3: optional i64 minVersion;

  /**
   *  The digest of the goal or actual state. If matches, the data field will
   *  be omitted in the return states.
   */
  4: optional string digest;
}


/** Raised when a given state already exists */
exception StateAlreadyExists
{
  1: optional string message;

  /** List of state IDs that already exist */
  2: optional list<StateKey> existingStates
}


/** Raised when a given state is not valid */
exception InvalidGoalState
{
  1: optional string message;

  /** List of invalid goal states */
  2: optional list<GoalState> states
}


/** 
 *  Rasied when a Thrift request invocation throws uncaught exception
 */
exception InternalServerError
{
  1: optional string message;
}


/**
 *  Goal State Service interface that will be implemented by a GoalState
 *  server and consumed by GoalState agents and clients.
 */
service GoalStateManager {

  /**
   *  Set the goal states for the list of instances. Will create the
   *  goal state if not exists. Otherwise, the existing goal state will
   *  be updated ( Append only ??).
   */
  void setGoalStates(1: list<GoalState> states) throws (
    1: StateAlreadyExists alreadyExists,
    2: InternalServerError serverError,
  )

  /**
   *  Query goal states for a list of service instances. Will omit the data
   *  field in the return GoalState if the digest matches.
   */
  
  list<GoalState> queryGoalStates(1: list<StateQuery> queries) throws (
    1: InternalServerError serverError,
  )

  /**
   *  Update the actual states for the list of instances. The goal state agent
   *  also uses this method as a way to keep-alive service instances in the
   *  master if the version number is the same. (Append only ??)
   */
  void updateActualStates(1: list<ActualState> states) throws (
    1: InternalServerError serverError,
  )

  /**
   *  Query actual states of a module for a list of service instances
   */
  
  list<ActualState> queryActualStates(1: list<StateQuery> queries) throws (
    1: InternalServerError serverError,
  )
}


/**
 *  Pluggable goal state module interface to be implemented by a goal state
 *  plugin such as config bundle manager. Note that the goal state module
 *  can be either running within the same process space as goal state agent
 *  or running in a separate process on the same host.
 */
service GoalStateModule {

  /** Get the module name */
  string getName()

  /** Apply the list of goal states for this module */
  void applyGoalStates(1: list<GoalState> states) throws (
    1: InvalidGoalState invalid,
  )

  /**
   *  Get the list of actual states for the service instances. Return the
   *  states for all instances if the list of instanceIds is empty.
   */
  list<ActualState> getActualStates(1: list<string> instanceIds)
}
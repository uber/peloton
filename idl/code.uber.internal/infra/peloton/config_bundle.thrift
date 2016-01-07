/**
 *  Config bundle Thrift interface
 */

namespace py peloton

/** Config bundle type */
enum ConfigBundleType
{
  UCONFIG = 1,
  TRANSLATION,
  FLIPR,
}


/**
 *  The config bundle state for a service instance on host. This
 *  could be either goal state or actual state. 
 */

struct ConfigBundleState
{
  /** Config bundle type */
  1: required ConfigBundleType type;

  /** Datacenter where config bundle to be deployed */
  2: required string datacenter;

  /** Pipeline where config bundle to be deployed */
  3: required string pipeline;
  
  /** Application ID of the config bundle, e.g. marge */
  4: required string appId;
  
  /** Deployment ID of the config bundle, e.g. production */
  5: required string deploymentId;

  /** Current active revision of the config bundle */
  6: optional string activeRevision;

  /** Next revision to which the config bundle is upgraded */
  7: optional string nextRevision;

  /**
   *  Past revisions of the config bundle which can be used for
   *  upgrade rollback
   */
  8: optional list<string> pastRevisions;

}
  
  
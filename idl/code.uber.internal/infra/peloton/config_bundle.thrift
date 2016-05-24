/**
 *  Config bundle Thrift interface
 */

namespace java com.uber.infra.peloton
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

  /** Datacenter where config bundle to be deployed, e.g. sjc1/dca1/pek1/pvg1 */
  2: required string datacenterId;

  /** Pipeline where config bundle to be deployed, e.g. us1/us2/cn1/cn2 */
  3: required string pipelineId;

  /** Service ID of the config bundle, e.g. homer */
  4: required string serviceId;

  /** Application ID of the config bundle, e.g. homer-kafka-ingester */
  5: required string appId;
  
  /** Deployment ID of the config bundle, e.g. production */
  6: required string deploymentId;

  /** Current active revision of the config bundle */
  7: optional string activeRevision;

  /** Next revision to which the config bundle is upgraded */
  8: optional string nextRevision;

  /**
   *  Past revisions of the config bundle which can be used for
   *  upgrade rollback
   */
  9: optional list<string> pastRevisions;

}
  
  
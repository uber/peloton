/**
 *  Common Thrift definitions
 */

namespace java com.uber.infra.peloton.common

typedef i64 (js.type = "Long") Long

/** DateTime in ISO format */
typedef string DateTime


/** Entity ID */
struct EntityID
{
  /** Type of the entity */
  1: required string type

  /** Unique ID of the entity. Either UUID or unique name. */
  2: required string id
}

/** Change log of the entity info */
struct ChangeLog
{
  /**
   *  Version number of the entity info which is monotonically increasing.
   *  Clients can use this to guide against race conditions using MVCC.
   */
  1: required Long version

  /** The timestamp when the entity info is created */
  2: required DateTime createdAt

  /** The timestamp when the entity info is updated */
  3: required DateTime updatedAt

  /** The entity of the user that updated the entity info */ 
  4: required string updatedBy
} 

/**
 * Key, value pair used to store free form user-data from upper layer.
 */
struct Label {
  1: required string key
  2: optional string value
}

/** Raised when a given entity already exists */
exception EntityAlreadyExists
{
  /** Entity ID that already exists */
  1: required EntityID existingEntity
  
  2: optional string message
}


/** Raised when a given entity does not exist */
exception EntityNotFound
{
  /** Entity ID that was not found */
  1: required EntityID entity
  
  2: optional string message
}

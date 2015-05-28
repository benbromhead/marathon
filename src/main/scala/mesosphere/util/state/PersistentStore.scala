package mesosphere.util.state

import scala.concurrent.Future

/**
  * An Entity that is provided and can be changed by the related store.
  */
trait PersistentEntity {

  /**
    * Get the content bytes of this entity.
    */
  def bytes: Array[Byte]

  /**
    * Mutate this entity with given bytes.
    */
  def mutate(bytes: Array[Byte]): PersistentEntity
}

/**
  * Store abstraction for different store implementations.
  */
trait PersistentStore {

  type ID = String

  /**
    * Fetch entity with given key identifier
    * @param key the identifier of this id
    * @return either the entity or a failure.
    *         In case of a storage specific failure, a StoreCommandFailedException is thrown.
    */
  def load(key: ID): Future[PersistentEntity]

  /**
    * Store the given entity.
    * @param entity the entity to store
    * @return either the entity or a failure.
    *         In case of a storage specific failure, a StoreCommandFailedException is thrown.
    */
  def save(entity: PersistentEntity): Future[PersistentEntity]

  /**
    * Expunge the entity with given id.
    * @param key the key identifier
    * @return either the entity or a failure.
    *         In case of a storage specific failure, a StoreCommandFailedException is thrown.
    */
  def delete(key: ID): Future[PersistentEntity]

  /**
    * List all available identifier.
    * @return the list of available identifier.
    */
  def allIds(): Future[Seq[ID]]
}

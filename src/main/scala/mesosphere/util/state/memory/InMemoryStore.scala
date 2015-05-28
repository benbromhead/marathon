package mesosphere.util.state.memory

import mesosphere.marathon.StoreCommandFailedException
import mesosphere.util.ThreadPoolContext
import mesosphere.util.state.{ PersistentEntity, PersistentStore }

import scala.concurrent.{ ExecutionContext, Future }

/**
  * Simple in memory store implementation.
  * This is intended only for tests - do not use in production!
  * @param ec the execution context to use.
  */
class InMemoryStore(implicit val ec: ExecutionContext = ThreadPoolContext.context) extends PersistentStore {

  var entities = Map.empty[ID, InMemoryEntity]

  override def load(key: ID): Future[PersistentEntity] = Future.successful(
    entities.getOrElse(key, InMemoryEntity(key))
  )

  override def save(entity: PersistentEntity): Future[PersistentEntity] = {
    entity match {
      case e @ InMemoryEntity(id, _) =>
        entities += id -> e
        Future.successful(e)
      case _ => Future.failed(new IllegalArgumentException(s"Wrong entity type: $entity"))
    }
  }

  override def delete(key: ID): Future[PersistentEntity] = {
    entities.get(key) match {
      case Some(value) =>
        entities -= key
        Future.successful(value)
      case None => Future.failed(new StoreCommandFailedException("No entity with this id"))
    }
  }

  override def allIds(): Future[Seq[ID]] = Future.successful(entities.keySet.toSeq)
}

case class InMemoryEntity(id: String, bytes: Array[Byte] = Array.empty) extends PersistentEntity {
  override def mutate(bytes: Array[Byte]): PersistentEntity = copy(bytes = bytes)
}

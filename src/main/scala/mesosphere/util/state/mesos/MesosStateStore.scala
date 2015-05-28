package mesosphere.util.state.mesos

import mesosphere.util.BackToTheFuture.Timeout
import mesosphere.util.ThreadPoolContext
import mesosphere.util.state.{ PersistentEntity, PersistentStore }
import org.apache.mesos.state.{ Variable, State }

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

class MesosStateStore(state: State, timeout: Duration) extends PersistentStore {

  implicit val timeoutDuration = Timeout(timeout)
  implicit val ec = ThreadPoolContext.context
  import mesosphere.util.BackToTheFuture.futureToFuture

  override def load(key: ID): Future[PersistentEntity] = futureToFuture(state.fetch(key)).map(MesosStateEntity)

  override def save(entity: PersistentEntity): Future[PersistentEntity] = entity match {
    case MesosStateEntity(v) => futureToFuture(state.store(v)).map(MesosStateEntity)
    case _                   => throw new IllegalArgumentException("Can not handle this kind of entity")
  }

  override def delete(key: ID): Future[PersistentEntity] = {
    futureToFuture(state.fetch(key)).flatMap { variable =>
      futureToFuture(state.expunge(variable)).map(_ => MesosStateEntity(variable))
    }
  }

  override def allIds(): Future[Seq[ID]] = futureToFuture(state.names()).map(_.asScala.toSeq)
}

case class MesosStateEntity(variable: Variable) extends PersistentEntity {
  override def bytes: Array[Byte] = variable.value()
  override def mutate(bytes: Array[Byte]): PersistentEntity = copy(variable = variable.mutate(bytes))
}

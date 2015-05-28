package mesosphere.util.state.zk

import java.util.UUID

import com.fasterxml.uuid.impl.UUIDUtil
import com.twitter.zk.ZNode.Data
import com.twitter.zk.{ ZkClient, ZNode }
import mesosphere.marathon.{ StoreCommandFailedException, Protos }
import mesosphere.util.state.{ PersistentEntity, PersistentStore }
import com.twitter.util.{ Await, Throw, Return, Future => TWFuture }
import ZKStore._
import org.apache.log4j.Logger
import com.google.protobuf.ByteString
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.concurrent.{ Promise, Future }

class ZKStore(client: ZkClient, rootNode: ZNode) extends PersistentStore {

  private[this] val log = Logger.getLogger(getClass)

  val root = createNode(rootNode)

  /**
    * Fetch data and return entity.
    * The entity is returned also if it is not found in zk, since it is needed for the store operation.
    */
  override def load(key: ID): Future[ZKEntity] = {
    val node = root(key)
    require(node.parent == root, "Nested paths are not supported!")
    node.getData().liftToTry.map {
      case Return(data)               => ZKEntity(node, ZKData(data.bytes), Some(data))
      case Throw(ex: NoNodeException) => ZKEntity(node, ZKData(key, UUID.randomUUID()))
      case Throw(ex: KeeperException) => throw new StoreCommandFailedException(s"Can not fetch $key", ex)
      case Throw(ex)                  => throw ex
    }.asScala
  }

  /**
    * This will store a previously fetched entity.
    * The entity will be either created or updated, depending on the read state.
    * @return Some value, if the store operation is successful otherwise None
    */
  override def save(entity: PersistentEntity): Future[ZKEntity] = {
    val zk = zkEntity(entity)
    def update(nodeData: ZNode.Data): Future[ZKEntity] = {
      val future: TWFuture[Data] = zk.node.setData(zk.data.toProto.toByteArray, nodeData.stat.getVersion)
      mapFutureResult(future, s"Can not update entity $entity") { data =>
        TWFuture.value(zk.copy(read = Some(data)))
      }
    }
    def create(): Future[ZKEntity] = {
      mapFutureResult(zk.node.create(zk.data.toProto.toByteArray), s"Can not update entity $entity") { data =>
        zk.node.getData().map{ data => zk.copy(read = Some(data)) }
      }
    }
    zk.read.map(update).getOrElse(create())
  }

  /**
    * Delete an entry with given identifier.
    */
  override def delete(key: ID): Future[ZKEntity] = {
    val node = root(key)
    require(node.parent == root, "Nested paths are not supported!")
    mapFutureResult(node.exists(), s"Can not delete entity $key") { data =>
      node.delete(data.stat.getVersion).map(n => ZKEntity(n, ZKData(key, UUID.randomUUID())))
    }
  }

  override def allIds(): Future[Seq[ID]] = {
    root.getChildren().map(_.children.map(_.name)).asScala
  }

  private def mapFutureResult[A, B](value: TWFuture[A], errorMessage: String)(fn: A => TWFuture[B]): Future[B] = {
    value.liftToTry.flatMap {
      case Return(data)               => fn(data)
      case Throw(ex: KeeperException) => throw new StoreCommandFailedException(errorMessage, ex)
      case Throw(ex)                  => throw ex
    }.asScala
  }

  private def zkEntity(entity: PersistentEntity): ZKEntity = {
    entity match {
      case zk: ZKEntity => zk
      case _            => throw new IllegalArgumentException("Can not handle this kind of entity")
    }
  }

  private def createNode(node: ZNode): ZNode = {
    Await.result(node.exists().liftToTry.flatMap {
      case Return(data) => TWFuture.value(node)
      case Throw(ex)    => node.create()
    })
  }
}

case class ZKEntity(node: ZNode, data: ZKData, read: Option[ZNode.Data] = None) extends PersistentEntity {
  override def mutate(updated: Array[Byte]): PersistentEntity = copy(data = data.copy(bytes = updated))
  override def bytes: Array[Byte] = data.bytes
}

case class ZKData(name: String, uuid: UUID, bytes: Array[Byte] = Array.empty) {
  def toProto: Protos.ZKStoreEntry = Protos.ZKStoreEntry.newBuilder()
    .setName(name)
    .setUuid(ByteString.copyFromUtf8(uuid.toString))
    .setValue(ByteString.copyFrom(bytes))
    .build()
}
object ZKData {
  def apply(bytes: Array[Byte]): ZKData = {
    val proto = Protos.ZKStoreEntry.parseFrom(bytes)
    new ZKData(proto.getName, UUIDUtil.uuid(proto.getUuid.toByteArray), proto.getValue.toByteArray)
  }
}

object ZKStore {
  implicit class Twitter2Scala[T](val twitterF: TWFuture[T]) extends AnyVal {
    def asScala: Future[T] = {
      val promise = Promise[T]()
      twitterF.onSuccess(promise.success(_))
      twitterF.onFailure(promise.failure(_))
      promise.future
    }
  }
}

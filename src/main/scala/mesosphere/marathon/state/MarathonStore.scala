package mesosphere.marathon.state

import com.codahale.metrics.MetricRegistry.name
import com.codahale.metrics.{ Histogram, MetricRegistry }
import com.google.protobuf.InvalidProtocolBufferException
import mesosphere.marathon.MarathonConf
import mesosphere.util.state.PersistentStore
import mesosphere.util.{ LockManager, ThreadPoolContext }
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class MarathonStore[S <: MarathonState[_, S]](
    conf: MarathonConf,
    store: PersistentStore,
    registry: MetricRegistry,
    newState: () => S,
    prefix: String = "app:") extends PersistenceStore[S] {

  import ThreadPoolContext.context

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] lazy val locks = LockManager[String]()

  def contentClassName: String = newState().getClass.getSimpleName

  protected[this] val bytesRead: Histogram = registry.histogram(name(getClass, contentClassName, "read-data-size"))
  protected[this] val bytesWritten: Histogram = registry.histogram(name(getClass, contentClassName, "write-data-size"))

  def fetch(key: String): Future[Option[S]] = {
    store.load(prefix + key) map { entity =>
      bytesRead.update(entity.bytes.length)
      try {
        Some(stateFromBytes(entity.bytes))
      }
      catch {
        case e: InvalidProtocolBufferException =>
          if (entity.bytes.nonEmpty) {
            log.error(s"Failed to read $key, could not deserialize data (${entity.bytes.length} bytes).", e)
          }
          None
      }
    }
  }

  def modify(key: String)(f: (() => S) => S): Future[Option[S]] = {
    val lock = locks.get(key)
    lock.acquire()

    val res: Future[Option[S]] = store.load(prefix + key) flatMap { entity =>
      bytesRead.update(entity.bytes.length)
      val deserialize = { () =>
        try {
          stateFromBytes(entity.bytes)
        }
        catch {
          case e: InvalidProtocolBufferException =>
            if (entity.bytes.nonEmpty) {
              log.error(s"Failed to read $key, could not deserialize data (${entity.bytes.length} bytes).", e)
            }
            newState()
        }
      }
      val newValue: S = f(deserialize)
      store.save(entity.mutate(newValue.toProtoByteArray)) map { newVar =>
        bytesWritten.update(newVar.bytes.length)
        Some(stateFromBytes(newVar.bytes))
      }
    }

    res onComplete { _ =>
      lock.release()
    }

    res
  }

  def expunge(key: String): Future[Boolean] = {
    val lock = locks.get(key)
    lock.acquire()

    val res = store.delete(prefix + key) map { _ => true }

    res onComplete { _ =>
      lock.release()
    }

    res
  }

  def names(): Future[Seq[String]] = {
    store.allIds().map {
      _.collect {
        case name if name startsWith prefix => name.replaceFirst(prefix, "")
      }
    }
  }

  private def stateFromBytes(bytes: Array[Byte]): S = {
    newState().mergeFromProto(bytes)
  }
}

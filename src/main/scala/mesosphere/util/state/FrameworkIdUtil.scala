package mesosphere.util.state

import org.apache.mesos.Protos.{ FrameworkInfo => FrameworkInfoProto, FrameworkID => FrameworkIDProto }
import com.google.protobuf.InvalidProtocolBufferException
import java.util.logging.{ Level, Logger }
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }
import scala.concurrent.{ Future, Await, ExecutionContext }

/**
  * Utility class for keeping track of a framework ID in Mesos state.
  *
  * @param state State implementation
  * @param key The key to store the framework ID under
  */

class FrameworkIdUtil(val state: PersistentStore, val key: String = "frameworkId") {

  private val log = Logger.getLogger(getClass.getName)

  def fetch(implicit ec: ExecutionContext, timeout: Duration): Option[FrameworkIDProto] = {
    val f: Future[Option[FrameworkIDProto]] = state.load(key).map {
      case variable if variable.bytes.length > 0 =>
        try {
          val frameworkId = FrameworkIDProto.parseFrom(variable.bytes)
          Some(frameworkId)
        }
        catch {
          case e: InvalidProtocolBufferException =>
            log.warning("Failed to parse framework ID")
            None
        }
      case _ => None
    }
    Await.result(f, timeout)
  }

  def store(frameworkId: FrameworkIDProto)(implicit ec: ExecutionContext, timeout: Duration) {
    state.load(key).map { oldVariable =>
      val newVariable = oldVariable.mutate(frameworkId.toByteArray)
      state.save(newVariable).onComplete {
        case Success(_) =>
          log.info("Stored framework ID '%s'".format(frameworkId.getValue))
        case Failure(t) =>
          log.log(Level.WARNING, "Failed to store framework ID", t)
      }
    }
  }

  def setIdIfExists(frameworkInfo: FrameworkInfoProto.Builder)(implicit ec: ExecutionContext, timeout: Duration) {
    fetch match {
      case Some(id) =>
        log.info("Setting framework ID to %s".format(id.getValue))
        frameworkInfo.setId(id)
      case None =>
        log.info("No previous framework ID found")
    }
  }
}


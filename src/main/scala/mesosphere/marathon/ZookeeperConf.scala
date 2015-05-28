package mesosphere.marathon

import java.net.InetSocketAddress

import com.twitter.zk.AuthInfo
import org.rogach.scallop.ScallopConf

import scala.concurrent.duration._

trait ZookeeperConf extends ScallopConf {

  private val userAndPass = """[^/@]+"""
  private val hostAndPort = """[A-z0-9-.]+(?::\d+)?"""
  private val zkNode = """[^/]+"""
  private val zkURLPattern = s"""^zk://(?:$userAndPass@)?($hostAndPort(?:,$hostAndPort)*)(/$zkNode(?:/$zkNode)*)$$""".r

  lazy val zooKeeperTimeout = opt[Long]("zk_timeout",
    descr = "The timeout for ZooKeeper in milliseconds",
    default = Some(10000L))

  lazy val zooKeeperUrl = opt[String]("zk",
    descr = "ZooKeeper URL for storing state. Format: zk://host1:port1,host2:port2,.../path",
    validate = (in) => zkURLPattern.pattern.matcher(in).matches(),
    default = Some("zk://localhost:2181/marathon")
  )

  lazy val zooKeeperMaxVersions = opt[Int]("zk_max_versions",
    descr = "Limit the number of versions, stored for one entity.",
    default = Some(25)
  )

  lazy val zooKeeperUser = opt[String]("zk_user",
    descr = "The login name used to authenticate against zookeeper",
    default = None
  )

  lazy val zooKeeperPassword = opt[String]("zk_pass",
    descr = "The password",
    default = None
  )

  dependsOnAll(zooKeeperPassword, List(zooKeeperUser))

  def zooKeeperStatePath(): String = "%s/state".format(zkPath)
  def zooKeeperLeaderPath(): String = "%s/leader".format(zkPath)
  def zooKeeperServerSetPath(): String = "%s/apps".format(zkPath)

  def zooKeeperHostAddresses: Seq[InetSocketAddress] =
    for (s <- zkHosts.split(",")) yield {
      val splits = s.split(":")
      require(splits.length == 2, "expected host:port for zk servers")
      new InetSocketAddress(splits(0), splits(1).toInt)
    }

  def zooKeeperAuth: Option[AuthInfo] = {
    (zooKeeperUser.get, zooKeeperPassword.get) match {
      case (Some(user), Some(pass)) => Some(AuthInfo.digest(user, pass))
      case _                        => None
    }
  }

  def zkURL: String = zooKeeperUrl.get.get

  lazy val zkHosts = zkURL match { case zkURLPattern(server, _) => server }
  lazy val zkPath = zkURL match { case zkURLPattern(_, path) => path }
  lazy val zkTimeoutDuration = Duration(zooKeeperTimeout(), MILLISECONDS)
}

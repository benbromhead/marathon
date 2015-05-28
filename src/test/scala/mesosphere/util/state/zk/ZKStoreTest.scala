package mesosphere.util.state.zk

import java.io.File

import com.twitter.zk.ZkClient
import mesosphere.marathon.StoreCommandFailedException
import mesosphere.marathon.integration.setup.{ IntegrationFunSuite, IntegrationTestConfig, ProcessKeeper }
import mesosphere.util.state.PersistentEntity
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.ZooDefs.Ids
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures._

import scala.collection.JavaConverters._

class ZKStoreTest extends IntegrationFunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  test("Root node gets read"){
    val store = zkStore
    store.allIds().futureValue should be(Seq.empty)
  }

  test("Fetch a non existing entity is possible") {
    val entity = fetch("notExistent")
    entity.read should be(None)
    entity.bytes should be(Array.empty)
  }

  test("Create node is successful") {
    val entity = fetch("foo")
    entity.read should be('empty)
    val stored = store(entity.mutate("Hello".getBytes))
    val read = fetch("foo")
    read.read should be('defined)
    read.data.bytes should be("Hello".getBytes)
  }

  test("Multiple creates should create only the first time") {
    val entity = fetch("foo2")
    entity.read should be('empty)
    zkStore.save(entity.mutate("Hello".getBytes)).futureValue.data.bytes should be("Hello".getBytes)
    whenReady(zkStore.save(entity.mutate("Hello again".getBytes)).failed) { _ shouldBe a[StoreCommandFailedException] }
  }

  test("Update node is successful") {
    val entity = fetch("foo")
    entity.read should be('empty)
    val stored = store(entity.mutate("Hello".getBytes))
    val read = fetch("foo")
    read.read should be('defined)
    read.data.bytes should be("Hello".getBytes)
    val update = store(read.mutate("Hello again".getBytes))
    update.read should be('defined)
    val readAgain = fetch("foo")
    readAgain.read should be('defined)
    readAgain.bytes should be("Hello again".getBytes)
  }

  test("Multiple updates will update only with correct version") {
    val entity = fetch("foo")
    entity.read should be('empty)
    val stored = store(entity.mutate("Hello".getBytes))
    val read = fetch("foo")
    read.read should be('defined)
    read.data.bytes should be("Hello".getBytes)
    zkStore.save(read.mutate("Hello again".getBytes)).futureValue.data.bytes should be("Hello again".getBytes)
    whenReady(zkStore.save(read.mutate("Will be None".getBytes)).failed) { _ shouldBe a[StoreCommandFailedException] }
    val readAgain = fetch("foo")
    readAgain.bytes should be("Hello again".getBytes)
  }

  test("Expunge on a non existing entry will succeed") {
    whenReady(zkStore.delete("notExistent").failed){ _ shouldBe a[StoreCommandFailedException] }
  }

  test("Expunge will delete an entry") {
    val entity = fetch("foo")
    entity.read should be('empty)
    val stored = store(entity.mutate("Hello".getBytes))
    val read = fetch("foo")
    read.read should be('defined)
    read.data.bytes should be("Hello".getBytes)
    val result = zkStore.delete("foo")
    result.futureValue.data.name should be("foo")
  }

  test("All ids in namespace can be listed") {
    zkStore.allIds().futureValue should be ('empty)
    store(fetch("foo").mutate("hello".getBytes))
    zkStore.allIds().futureValue should be (Seq("foo"))
  }

  def fetch(name: String): ZKEntity = zkStore.load(name).futureValue
  def store(entity: PersistentEntity): ZKEntity = zkStore.save(entity).futureValue

  lazy val zkStore: ZKStore = {
    implicit val timer = com.twitter.util.Timer.Nil
    import com.twitter.util.TimeConversions._
    val client = ZkClient(s"${config.zkHost}:${config.zkPort}", 10.minutes).withAcl(Ids.OPEN_ACL_UNSAFE.asScala)
    new ZKStore(client, client(config.zkPath))
  }

  //TODO: factor out zookeeper start/stop
  private var configOption: Option[IntegrationTestConfig] = None
  def config: IntegrationTestConfig = configOption.get

  override protected def beforeAll(configMap: ConfigMap): Unit = {
    FileUtils.deleteDirectory(new File("/tmp/foo"))
    configOption = Some(IntegrationTestConfig(configMap))
    if (!config.useExternalSetup) {
      ProcessKeeper.startZooKeeper(config.zkPort, "/tmp/foo")
    }
  }

  before {
    zkStore.allIds().futureValue.foreach { entry =>
      zkStore.delete(entry).futureValue
    }
  }
}

package com.stratio.provider.mongodb

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{ServerAddress, DBCollection, MongoClient, DBObject}
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.tests.MongosSystemForTestFactory
import de.flapdoodle.embed.process.runtime.Network

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Deploys an embedded cluster composed by:
 * - A mongo
 * - A list of config. servers
 * - A list of replica sets (Mongods)
 */
trait MongoClusterEmbedDatabase {

  //  Current system

  type Port = Int
  type Host = String
  type MB = Int
  type ReplicaSetName = String

  val configServerPorts: List[Port]
  val database: String
  val collection: String
  val shardKey: String
  val shardMaxSize: MB
  val chunkSize: MB
  val mongoPort: Port
  val currentHost: Host
  val replicaSets: Map[ReplicaSetName, List[Port]]

  lazy val databaseConnection = replicaSets.flatMap {
    case (replicaSet, port :: restPorts) =>
      s"$replicaSet/$currentHost:$port" :: restPorts.map { p =>
        s"$currentHost:$p"
      }
    case _ => ""
  }.mkString(",")

  lazy val system = new MongosSystemForTestFactory(
    mongoConfig(
      currentHost,
      configServerPorts.map(p => s"$currentHost:$p").mkString(","))(mongoPort),
    replicaSets.map {
      case (rs, ports) =>
        rs -> shardConfig(rs, ports.map(p => (currentHost, p))).asJava
    },
    configServerPorts.map(mongodConfig(currentHost, _)),
    database,
    collection,
    shardKey)

  //  Config builders

  private def mongoConfig(
    host: String,
    databaseLocation: String)(mongoPort: Int) =
    new MongosConfigBuilder()
      .version(Version.Main.PRODUCTION)
      .net(new Net(host, mongoPort, Network.localhostIsIPv6()))
      .configDB(databaseLocation)
      .build()

  private def mongodConfig(host: String, mongodPort: Int) =
    new MongodConfigBuilder()
      .version(Version.Main.PRODUCTION)
      .configServer(true)
      .net(new Net(host, mongodPort, Network.localhostIsIPv6()))
      .build()

  private def shardConfig(
    replicaSet: String,
    shardPorts: List[(String, Int)]): List[IMongodConfig] =
    shardPorts.map { case (host, port) =>
      new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .replication(new Storage(null, replicaSet, shardMaxSize))
        .net(new Net(host, port, Network.localhostIsIPv6()))
        .build()
    }

  //  Helpers

  protected def withCluster[T](
    f: MongosSystemForTestFactory => T): T = {
    system.start()
    val mongo = client()
    mongo.getDB(database).command(MongoDBObject("chunksize" -> chunkSize))
    val t = f(system)
    system.stop()
    t
  }

  protected def client(): MongoClient =
    new MongoClient(
      replicaSets.values.flatMap(ports =>
        ports.map(port =>
          new ServerAddress(currentHost, port))).toList)


  protected def populateDatabase(dataset: List[DBObject]) {
    import scala.collection.JavaConverters._

    val mongo = client()
    val col: DBCollection = mongo.getDB(database).getCollection(collection)
    col.insert(dataset.asJava)
  }

}
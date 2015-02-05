package com.stratio.deep.mongodb.schema

import com.mongodb.{DBCollection, DBObject, MongoClient}
import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net, RuntimeConfigBuilder}
import de.flapdoodle.embed.mongo.distribution.{IFeatureAwareVersion, Version}
import de.flapdoodle.embed.mongo.{Command, MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.config.IRuntimeConfig
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.process.runtime.Network

/**
 * Created by rmorandeira on 5/02/15.
 */
trait MongoEmbedDatabase {
  private val runtimeConfig = new RuntimeConfigBuilder()
    .defaults(Command.MongoD)
    .processOutput(ProcessOutput.getDefaultInstanceSilent())
    .build()

  protected def mongoStart(port: Int = 12345,
    version: IFeatureAwareVersion = Version.Main.PRODUCTION,
    runtimeConfig: IRuntimeConfig = runtimeConfig): MongodProps = {
    val mongodExe: MongodExecutable = mongodExec(port, version, runtimeConfig)
    MongodProps(mongodExe.start(), mongodExe)
  }

  protected def mongoStop( mongodProps: MongodProps ) = {
    Option(mongodProps).foreach( _.mongodProcess.stop() )
    Option(mongodProps).foreach( _.mongodExe.stop() )
  }

  protected def withEmbedMongoFixture(dataset: List[DBObject],
    port: Int = 12345,
    version: IFeatureAwareVersion = Version.Main.PRODUCTION,
    runtimeConfig: IRuntimeConfig = runtimeConfig)
      (fixture: MongodProps => Any) {
    val mongodProps = mongoStart(port, version, runtimeConfig)
    populateDatabase(port, dataset)
    try { fixture(mongodProps) } finally { Option(mongodProps).foreach( mongoStop ) }
  }

  private def runtime(config: IRuntimeConfig): MongodStarter = MongodStarter.getInstance(config)

  private def mongodExec(port: Int, version: IFeatureAwareVersion, runtimeConfig: IRuntimeConfig): MongodExecutable =
    runtime(runtimeConfig).prepare(
      new MongodConfigBuilder()
        .version(version)
        .net(new Net(port, Network.localhostIsIPv6()))
        .build()
    )

  private def populateDatabase(port: Int, dataset: List[DBObject]) = {
    import scala.collection.JavaConverters._

    val mongo: MongoClient = new MongoClient("localhost", port)
    val col: DBCollection = mongo.getDB("testDb").getCollection("testCol")
    col.insert(dataset.asJava)
  }
}

sealed case class MongodProps(mongodProcess: MongodProcess, mongodExe: MongodExecutable)
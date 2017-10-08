package org.apache.spark.sql.hive

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext, TestHiveSessionState, TestHiveSparkSession}
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.scalatest.BeforeAndAfterAll

trait TestHiveSpatialSingleton extends SparkFunSuite with BeforeAndAfterAll {
  protected val spark: SparkSession = TestHiveSpatial.sparkSession
  protected val hiveContext: TestHiveContext = TestHiveSpatial

  protected override def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }

}

object TestHiveSpatial
  extends TestHiveSpatialContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        .set("spark.sql.warehouse.dir", TestHiveContext.makeWarehouseDir().toURI.getPath)
        // SPARK-8910
        .set("spark.ui.enabled", "false")))

class TestHiveSpatialContext(@transient override val sparkSession: TestHiveSpatialSparkSession)
  extends TestHiveContext(sparkSession) {

  def this(sc: SparkContext, loadTestTables: Boolean = true) {
    this(new TestHiveSpatialSparkSession(HiveUtils.withHiveExternalCatalog(sc), None, loadTestTables))
  }
}

class TestHiveSpatialSessionState(sparkSession: TestHiveSparkSession)
  extends TestHiveSessionState(sparkSession) {

  self =>

  /**
    * Planner that takes into account Hive-specific strategies.
    */
  override def planner: SparkPlanner = {
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)
      with HiveStrategies {
      override val sparkSession: SparkSession = self.sparkSession

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++ Seq(
          FileSourceStrategy,
          DataSourceStrategy,
          DDLStrategy,
          SpecialLimits,
          InMemoryScans,
          HiveTableScans,
          DataSinks,
          Scripts,
          Aggregation,
          SpatialJoinSelection,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }
}

class TestHiveSpatialSparkSession(@transient private val sc: SparkContext,
                                  @transient private val existingSharedState: Option[SharedState],
                                  private val loadTestTables: Boolean)
  extends TestHiveSparkSession(sc, existingSharedState, loadTestTables) {

  self =>

  @transient
  override lazy val sessionState: TestHiveSessionState =
    new TestHiveSpatialSessionState(self)

  override def newSession(): TestHiveSparkSession = {
    new TestHiveSpatialSparkSession(sc, Some(sharedState), loadTestTables)
  }
}
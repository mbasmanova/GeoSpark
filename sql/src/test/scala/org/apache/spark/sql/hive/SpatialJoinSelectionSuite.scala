package org.apache.spark.sql.hive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class SpatialJoinSelectionSuite extends QueryTest with SQLTestUtils with TestHiveSpatialSingleton {

  val spatialFunctionNames = Seq(
    "ST_GeomFromText",
    "ST_Point",
    "ST_Contains",
    "ST_Intersects",
    "ST_Distance")

  private def withSpatialUdfs(f: => Unit): Unit = {
    try {
      spatialFunctionNames.foreach { name =>
        spark.sql(s"CREATE TEMPORARY FUNCTION $name AS 'com.esri.hadoop.hive.$name'")
      }

      f
    } finally {
      spatialFunctionNames.foreach { name =>
        spark.sql(s"DROP TEMPORARY FUNCTION $name")
      }
    }
  }

  private def getTable(plan: SparkPlan): MetastoreRelation = {
    val relations = plan.collect { case HiveTableScanExec(_, relation, _) => relation }
    assert(relations.size == 1, "Expected HiveTableScanExec, but got " + plan)
    relations(0)
  }

  private def getUdf(expression: Expression): HiveSimpleUDF = {
    val udfs = expression.collect { case udf: HiveSimpleUDF => udf }
    assert(udfs.size == 1, "Expected HiveSimpleUDF, but got " + expression)
    udfs(0)
  }

  private def findSpatialJoin(plan: SparkPlan): SpatialJoinExec = {
    val joins = plan.collect { case join: SpatialJoinExec => join }
    assert(joins.size == 1, "Should use spatial join")
    joins(0)
  }

  private def findDistanceJoin(plan: SparkPlan): DistanceJoinExec = {
    val joins = plan.collect { case join: DistanceJoinExec => join }
    assert(joins.size == 1, "Should use spatial distance join")
    joins(0)
  }

  private def verifySpatialJoin(plan: SparkPlan, intersects: Boolean,
                                expectExtraCondition: Boolean = false): Unit = {
    val join = findSpatialJoin(plan)

    val leftRelation = getTable(join.left);
    val leftUdf = getUdf(join.leftShape)

    assert(leftRelation.tableName == "polygons")
    assert(leftUdf.name == "ST_GeomFromText")
    assert(leftUdf.references.filter(leftRelation.attributes.contains(_)).isEmpty)

    val rightRelation = getTable(join.right);
    val rightUdf = getUdf(join.rightShape)

    assert(rightRelation.tableName == "points")
    assert(rightUdf.name == "ST_Point")
    assert(rightUdf.references.filter(rightRelation.attributes.contains(_)).isEmpty)

    assert(join.intersects == intersects)
    if (expectExtraCondition) {
      assert(join.extraCondition.isDefined)
    } else {
      assert(join.extraCondition == None)
    }
  }

  private def verifyDistanceJoin(plan: SparkPlan, leftTableName: String, rightTableName: String,
                                 expectExtraCondition: Boolean = false): Unit = {
    val join = findDistanceJoin(plan)

    val leftRelation = getTable(join.left);
    val leftUdf = getUdf(join.leftShape)

    assert(leftRelation.tableName == leftTableName)
    assert(leftUdf.name == "ST_Point")
    assert(leftUdf.references.filter(leftRelation.attributes.contains(_)).isEmpty)
    assert(join.radius.references.filter(leftRelation.attributes.contains(_)).isEmpty)

    val rightRelation = getTable(join.right);
    val rightUdf = getUdf(join.rightShape)

    assert(rightRelation.tableName == rightTableName)
    assert(rightUdf.name == "ST_Point")
    assert(rightUdf.references.filter(rightRelation.attributes.contains(_)).isEmpty)

    if (expectExtraCondition) {
      assert(join.extraCondition.isDefined)
    } else {
      assert(join.extraCondition == None)
    }
  }

  test("spatial join") {
    withTable("points", "polygons") {
      withSpatialUdfs {
        sql("CREATE TABLE points(lon DOUBLE, lat DOUBLE, name STRING)")
        sql("CREATE TABLE polygons(city_id INT, geometry STRING)")

        verifySpatialJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM points a, polygons b
             |WHERE ST_Contains(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, false)

        // Swap tables
        verifySpatialJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM polygons b, points a
             |WHERE ST_Contains(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, false)

        verifySpatialJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM points a, polygons b
             |WHERE ST_Intersects(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, true)

        // Swap tables
        verifySpatialJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM polygons b, points a
             |WHERE ST_Intersects(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, true)
      }
    }
  }

  test("spatial distance join") {
    withTable("points_a", "points_b") {
      withSpatialUdfs {
        sql("CREATE TABLE points_a(lon DOUBLE, lat DOUBLE, name STRING)")
        sql("CREATE TABLE points_b(lon DOUBLE, lat DOUBLE, name STRING)")

        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Swap tables
        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_b b, points_a a
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Use expression over columns in 'a' for radius
        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * a.lon
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Swap tables
        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_b b, points_a a
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * a.lon
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b")

        // Use expression over columns in 'b' for radius
        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * b.lon
           """.stripMargin).queryExecution.sparkPlan, "points_b", "points_a")

        // Swap tables
        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_b b, points_a a
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1 * b.lon
           """.stripMargin).queryExecution.sparkPlan, "points_b", "points_a")
      }
    }
  }

  test("spatial join with extra condition") {
    withTable("points", "polygons") {
      withSpatialUdfs {
        sql("CREATE TABLE points(lon DOUBLE, lat DOUBLE, name STRING)")
        sql("CREATE TABLE polygons(city_id INT, name STRING, geometry STRING)")

        verifySpatialJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM points a, polygons b
             |WHERE ST_Contains(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
             |  AND a.name <> b.name
         """.stripMargin).queryExecution.sparkPlan, false, true)

        verifySpatialJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.geometry
             |FROM points a, polygons b
             |WHERE a.name <> b.name
             |  AND ST_Contains(ST_GeomFromText(b.geometry), ST_Point(a.lon, a.lat))
         """.stripMargin).queryExecution.sparkPlan, false, true)
      }
    }
  }

  test("spatial distance join with extra condition") {
    withTable("points_a", "points_b") {
      withSpatialUdfs {
        sql("CREATE TABLE points_a(lon DOUBLE, lat DOUBLE, name STRING)")
        sql("CREATE TABLE points_b(lon DOUBLE, lat DOUBLE, name STRING)")

        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1
             |  AND a.name <> b.name
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b", true)

        verifyDistanceJoin(sql(
          s"""
             |SELECT a.name, a.lat, a.lon, b.name
             |FROM points_a a, points_b b
             |WHERE a.name <> b.name
             |  AND ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 0.1
           """.stripMargin).queryExecution.sparkPlan, "points_a", "points_b", true)
      }
    }
  }

}

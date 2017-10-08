package org.apache.spark.sql.hive

import com.esri.hadoop.hive.ST_AsText
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.internal.Logging
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams

/**
  *  ST_Contains(left, right) - left contains right
  *  or
  *  ST_Intersects(left, right) - left and right intersect
  *
  * @param left left side of the join
  * @param right right side of the join
  * @param leftShape expression for the first argument of ST_Contains or ST_Intersects
  * @param rightShape expression for the second argument of ST_Contains or ST_Intersects
  * @param intersects boolean indicating whether spatial relationship is 'intersects' (true)
  *                   or 'contains' (false)
  */
case class SpatialJoinExec(left: SparkPlan,
                           right: SparkPlan,
                           leftShape: Expression,
                           rightShape: Expression,
                           intersects: Boolean,
                           extraCondition: Option[Expression] = None)
    extends BinaryExecNode
    with SpatialJoin
    with Logging {}

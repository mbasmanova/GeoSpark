package org.apache.spark.sql.hive

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Plans `SpatialJoinExec` for inner joins on spatial relationships ST_Contains(a, b)
  * and ST_Intersects(a, b).
  *
  * Plans `DistanceJoinExec` for inner joins on spatial relationship ST_Distance(a, b) < r.
  */
object SpatialJoinSelection extends Strategy {

  /**
    * Returns true if specified expression has at least one reference and all its references
    * map to the output of the specified plan.
    */
  private def matches(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.find(plan.outputSet.contains(_)).isDefined &&
      expr.references.find(!plan.outputSet.contains(_)).isEmpty

  private def matchExpressionsToPlans(exprA: Expression,
                                      exprB: Expression,
                                      planA: LogicalPlan,
                                      planB: LogicalPlan): Option[(LogicalPlan, LogicalPlan)] =
    if (matches(exprA, planA) && matches(exprB, planB)) {
      Some((planA, planB))
    } else if (matches(exprA, planB) && matches(exprB, planA)) {
      Some((planB, planA))
    } else {
      None
    }

  private def findSpatialRelationship(
      exprA: Expression,
      exprB: Expression,
      spatialUdfName: String): Option[(Seq[Expression], Expression)] =
    exprA match {
      case HiveGenericUDF(spatialUdfName, _, children) =>
        Some((children, exprB))
      case _ =>
        exprB match {
          case HiveGenericUDF(spatialUdfName, _, children) =>
            Some((children, exprA))
          case _ =>
            None
        }
    }

  private def findDistanceRelationship(
      exprA: Expression,
      exprB: Expression): Option[(Seq[Expression], Expression, Expression)] =
    exprA match {
      case LessThanOrEqual(HiveSimpleUDF("ST_Distance", _, children), radius) =>
        Some((children, radius, exprB))
      case _ =>
        exprB match {
          case LessThanOrEqual(HiveSimpleUDF("ST_Distance", _, children), radius) =>
            Some((children, radius, exprA))
          case _ =>
            None
        }
    }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // ST_Contains(a, b) - a contains b
    case Join(left, right, Inner, Some(HiveGenericUDF("ST_Contains", _, children))) =>
      planSpatialJoin(left, right, children, false)

    // ST_Intersects(a, b) - a and b intersect
    case Join(left, right, Inner, Some(HiveGenericUDF("ST_Intersects", _, children))) =>
      planSpatialJoin(left, right, children, true)

    // ST_Distance(a, b) <= radius
    case Join(left,
              right,
              Inner,
              Some(LessThanOrEqual(HiveSimpleUDF("ST_Distance", _, children), radius))) =>
      planDistanceJoin(left, right, children, radius)

    case Join(left, right, Inner, Some(And(a, b))) =>
      findSpatialRelationship(a, b, "ST_Contains")
        .map {
          case (children, extraCondition) =>
            planSpatialJoin(left, right, children, false, Some(extraCondition))
        }
        .getOrElse(
          findSpatialRelationship(a, b, "ST_Intersects")
            .map {
              case (children, extraCondition) =>
                planSpatialJoin(left, right, children, true, Some(extraCondition))
            }
            .getOrElse(
              findDistanceRelationship(a, b)
                .map {
                  case (children, radius, extraCondition) =>
                    planDistanceJoin(left, right, children, radius, Some(extraCondition))
                }
                .getOrElse(Nil)
            )
        )

    case _ =>
      Nil
  }

  private def planSpatialJoin(left: LogicalPlan,
                              right: LogicalPlan,
                              children: Seq[Expression],
                              intersects: Boolean,
                              extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    val relationship = if (intersects) "ST_Intersects" else "ST_Contains";

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((planA, planB)) =>
        logInfo(s"Planning spatial join for $relationship relationship")
        SpatialJoinExec(planLater(planA), planLater(planB), a, b, intersects, extraCondition) :: Nil
      case None =>
        logInfo(
          s"Spatial join for $relationship with arguments not aligned " +
            "with join relations is not supported")
        Nil
    }
  }

  private def planDistanceJoin(left: LogicalPlan,
                               right: LogicalPlan,
                               children: Seq[Expression],
                               radius: Expression,
                               extraCondition: Option[Expression] = None): Seq[SparkPlan] = {
    val a = children.head
    val b = children.tail.head

    matchExpressionsToPlans(a, b, left, right) match {
      case Some((planA, planB)) =>
        if (radius.references.isEmpty || matches(radius, planA)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planA), planLater(planB), a, b, radius, extraCondition) :: Nil
        } else if (matches(radius, planB)) {
          logInfo("Planning spatial distance join")
          DistanceJoinExec(planLater(planB), planLater(planA), b, a, radius, extraCondition) :: Nil
        } else {
          logInfo(
            "Spatial distance join for ST_Distance with non-scalar radius " +
              "that is not a computation over just one side of the join is not supported")
          Nil
        }
      case None =>
        logInfo(
          "Spatial distance join for ST_Distance with arguments not " +
            "aligned with join relations is not supported")
        Nil
    }
  }
}

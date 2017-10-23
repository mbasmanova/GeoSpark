package org.datasyslab.geospark.monitoring

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}

import scala.collection.mutable

class GeoSparkListener extends SparkListener {
  private val _taskCpuTime: mutable.Map[Integer, Long] = mutable.Map()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.reason == org.apache.spark.Success) {
      val cpuTime = taskEnd.taskMetrics.executorCpuTime
      _taskCpuTime(Integer.parseInt(taskEnd.taskInfo.id.split('.')(0))) = cpuTime
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = {
    val accumulables = stageCompleted.stageInfo.accumulables

    def getCounterOption(name: String) = {
      accumulables.find { case (k, v) => v.name == Some("geospark.spatialjoin." + name) }
    }

    def getCounter(name: String) = {
      getCounterOption(name).get._2.value.get
    }

    if (getCounterOption("buildCount").isDefined) {

      val buildCounts: Map[Int, Long] = getCounter("buildCount").asInstanceOf[Map[Int, Long]]
      val streamCounts: Map[Int, Long] = getCounter("streamCount").asInstanceOf[Map[Int, Long]]
      val candidateCounts: Map[Int, Long] = getCounter("candidateCount").asInstanceOf[Map[Int, Long]]

      val stats: List[(Int, Long, Long, Long, Long)] =
        buildCounts.map {
          case (partitionId, buildCount) => {
            val streamCount: Long = streamCounts.getOrElse(partitionId, -1)
            val candidateCount: Long = candidateCounts.getOrElse(partitionId, -1)
            val cpuTime: Long = _taskCpuTime.getOrElse(partitionId, -1)
            (partitionId, buildCount, streamCount, candidateCount, cpuTime)
          }

        }.toList.sortBy {
          case (_, _, _, _, cpuTime) => cpuTime
        }

      Console.out.println("--------------")
      stats.foreach {
        case (partitionId, buildCount, streamCount, candidateCount, cpuTime) =>
          Console.out.println(f"PID: $partitionId% 3d,\t ${cpuTime / 1000}% 10d s, $buildCount% 6d, $streamCount% 6d, $candidateCount% 6d")
      }
    }
  }
}

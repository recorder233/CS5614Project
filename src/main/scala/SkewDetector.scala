import main.sc
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}

import scala.collection.mutable.ArrayBuffer

class SkewDetector extends SparkListener{
  private var stages = Map.empty[Int, Stage]
  var logTask = true
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    stages(taskEnd.stageId).completeTask(taskEnd)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    super.onTaskStart(taskStart)
    stages(taskStart.stageId).addTask(new Task(taskStart))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stages(stageCompleted.stageInfo.stageId).complete(stageCompleted)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stages = stages +(stageSubmitted.stageInfo.stageId -> new Stage(stageSubmitted))
  }

}

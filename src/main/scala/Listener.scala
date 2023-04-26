import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}

class Listener extends SparkListener{
  var stages = Map.empty[Int, Stage]

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    //super.onTaskEnd(taskEnd)
    println("Task finished: belongs to Stage: " + taskEnd.stageId)
    stages(taskEnd.stageId).addTask(new Task(taskEnd))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    //super.onStageCompleted(stageCompleted)
    stages(stageCompleted.stageInfo.stageId).complete(stageCompleted)

    println("Stage finished: " + stageCompleted.stageInfo.name)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    //super.onStageSubmitted(stageSubmitted)
    println("Stage started: " + stageSubmitted.stageInfo.name)
    stages = stages +(stageSubmitted.stageInfo.stageId -> new Stage(stageSubmitted))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    stages.foreach(println)
  }
}

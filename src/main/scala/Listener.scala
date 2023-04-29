import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}

import scala.collection.mutable.ArrayBuffer

class Listener extends SparkListener{
  var stages = Map.empty[Int, Stage]
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    //super.onTaskEnd(taskEnd)
    println("Task finished: belongs to Stage: " + taskEnd.stageId + " Executor: " + taskEnd.taskInfo.executorId + " Used: " + taskEnd.taskInfo.duration+" s, read: " + taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten)
    stages(taskEnd.stageId).addTask(new Task(taskEnd))
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    super.onTaskStart(taskStart)

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    //super.onStageCompleted(stageCompleted)
    stages(stageCompleted.stageInfo.stageId).complete(stageCompleted)
    println("Stage finished: " + stageCompleted.stageInfo.name)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    //super.onStageSubmitted(stageSubmitted)
    println("Stage started: " + stageSubmitted.stageInfo.name)
    println(stages.values)
    stages = stages +(stageSubmitted.stageInfo.stageId -> new Stage(stageSubmitted))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    findUnevenStages(stages)
  }

  private def findUnevenStages(stages: Map[Int, Stage]) = {
    for ((key, stage) <- stages) {
      // Access the key and value and perform desired actions
      if(findSkew(stage)){
        //println("possible skew at: " +stage.getName())
        if(checkDataSkew(stage)){
          println("possible data skew in: " + stage.getName())
        }
        else{
          println("possible other skew in: " + stage.getName())
        }
      }
    }
  }

  private def findSkew(stage: Stage): Boolean = {
    val tasks = stage.getTasks()
    if(tasks.length == 1){
      return false
    }
    if (tasks.length == 2){
      return math.abs(tasks(0).getEndTime() - tasks(1).getEndTime()) > 100
    }
    val average = tasks.map(_.getEndTime()).sum / tasks.length
    val std = math.sqrt(tasks.map(task => math.pow(task.getEndTime() - average, 2)).sum / tasks.length)
    for (task <- tasks){
      if(task.getEndTime() > average + std){
        return true
      }
    }
    return false
  }

  private def checkDataSkew(stage: Stage): Boolean = {
    val tasks = stage.getTasks()
    if(tasks.length == 2){
      return math.abs(tasks(0).getReadSize() - tasks(1).getReadSize()) > 10 * 1024
    }
    val average = tasks.map(_.getReadSize()).sum / tasks.length
    val variance = tasks.map(task => math.pow(task.getReadSize() - average, 2)).sum / tasks.length
    val std = math.sqrt(variance)
    for (task <- tasks){
      if(task.getReadSize() < average -std || task.getReadSize() > average + std){
        return true
      }
    }
    return false
  }
}

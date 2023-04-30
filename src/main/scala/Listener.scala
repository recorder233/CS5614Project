import main.sc
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}

import scala.collection.mutable.ArrayBuffer

class Listener extends SparkListener{
  var stages = Map.empty[Int, Stage]
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    //super.onTaskEnd(taskEnd)
    println("Task " + taskEnd.taskInfo.taskId +" finished: belongs to Stage: " + taskEnd.stageId + " Used: " + taskEnd.taskInfo.duration+"s read: " + (taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead / 1024) + "KiB")
    stages(taskEnd.stageId).completeTask(taskEnd)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    super.onTaskStart(taskStart)
    stages(taskStart.stageId).addTask(new Task(taskStart))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    //super.onStageCompleted(stageCompleted)
    stages(stageCompleted.stageInfo.stageId).complete(stageCompleted)
    //println("Stage finished: " + stageCompleted.stageInfo.stageId)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    //super.onStageSubmitted(stageSubmitted)
    //println("Stage started: " + stageSubmitted.stageInfo.name)
    //println(stages.values)
    stages = stages +(stageSubmitted.stageInfo.stageId -> new Stage(stageSubmitted))
  }

//  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
//    findUnevenStages(stages)
//  }

//  private def findUnevenStages(stages: Map[Int, Stage]) = {
//    for ((key, stage) <- stages) {
//      // Access the key and value and perform desired actions
//      if(findSkew(stage)){
//        //println("possible skew at: " +stage.getName())
//        if(checkDataSkew(stage)){
//          println("possible data skew in: " + stage.getName())
//        }
//        else{
//          println("possible other skew in: " + stage.getName())
//        }
//      }
//    }
//  }

}

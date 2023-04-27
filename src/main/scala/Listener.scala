import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}

import scala.collection.mutable.ArrayBuffer

class Listener extends SparkListener{
  var stages = Map.empty[Int, Stage]
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    //super.onTaskEnd(taskEnd)
    println("Task finished: belongs to Stage: " + taskEnd.stageId + " Used: " + taskEnd.taskInfo.duration+" s, read: " + taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten)
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
    println(stages.values)
    stages = stages +(stageSubmitted.stageInfo.stageId -> new Stage(stageSubmitted))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    var stagesExecutionSum: Long = 0
    val myList = ArrayBuffer[Long]()
    for (value <- stages.values) {
      stagesExecutionSum += value.getStageTime()
      myList += value.getStageTime()
    }
    val stagesLength = stages.size
    val averageExecutionTime: Double = stagesExecutionSum / stagesLength
    println("give me average Execution Time: " + averageExecutionTime)
    println("myList is " + myList.mkString(", "))
    val variance = myList.map(time => math.pow(time - averageExecutionTime, 2)).sum / stagesLength
    val std = math.sqrt(variance)
    println("give me std: " + std)
    val result = findUnevenStages(averageExecutionTime-std, averageExecutionTime+std)
    println("skew is happening in stage " + result.mkString(", "))
  }


  def findUnevenStages(min: Double, max: Double): ArrayBuffer[Int] = {
    val result = ArrayBuffer[Int]()
    for ((key, value) <- stages) {
      // Access the key and value and perform desired actions
      val time = value.getStageTime()
      if (time < min || time > max) {
        result += value.getId()
      }
    }
    result
  }
}

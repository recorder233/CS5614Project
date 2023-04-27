import org.apache.spark.scheduler.SparkListenerTaskEnd

class Task(){

  private var stageId = -1
  private var startTime: Long = -1
  private var endTime: Long = -1
  private var duration: Long = -1
  private var readSize: Long = 0

  def this(taskEnd: SparkListenerTaskEnd) = {
    this()
    stageId = taskEnd.stageId
    startTime = taskEnd.taskInfo.launchTime
    endTime = taskEnd.taskInfo.finishTime
    duration = endTime - startTime
    readSize = taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead
  }

  def getDuration() = duration

}

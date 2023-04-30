import org.apache.spark.TaskContext
import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListenerTaskStart}

class Task(){

  private var stageId = -1
  private var taskId: Long = -1
  private var startTime: Long = -1
  private var endTime: Long = -1
  private var duration: Long = -1
  private var readSize: Long = 0
  private var writeSize: Long = 0
  private var completed = false
  private var gcTime: Long = -1

  def this(taskStart: SparkListenerTaskStart) = {
    this()
    stageId = taskStart.stageId
    startTime = taskStart.taskInfo.launchTime
    taskId = taskStart.taskInfo.taskId
    //endTime = taskStart.taskInfo.finishTime
    //duration = endTime - startTime
    //readSize = taskStart.taskMetrics.shuffleReadMetrics.totalBytesRead
    //writeSize = taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten
  }

  def end(taskEnd: SparkListenerTaskEnd) ={
    endTime = taskEnd.taskInfo.finishTime
    duration = taskEnd.taskInfo.duration
    readSize = taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead
    writeSize = taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten
    gcTime = taskEnd.taskMetrics.jvmGCTime
    completed = true

  }

  def getDuration() = duration

  def getReadSize() = readSize

  def getWriteSize() = writeSize

  def getEndTime() = endTime

  def getTaskId() = taskId

  def getGcTime() = gcTime

}

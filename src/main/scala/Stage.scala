import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}

class Stage(){
  private var tasks =Array.empty[Task]
  private var name = ""
  private var id = -1
  private var completed = false
  private var startTime: Long = -1
  private var completeTime: Long = -1
  def this(stageSubmitted: SparkListenerStageSubmitted)= {
    this()
    name = stageSubmitted.stageInfo.name
    id = stageSubmitted.stageInfo.stageId
    startTime = stageSubmitted.stageInfo.submissionTime.get
  }

  def addTask(task: Task) ={
    tasks = tasks :+ task
  }

  def getId() : Int = id
  def getTasks(): Array[Task]= tasks

  def getStageTime(): Long = completeTime - startTime

  def complete(stageCompleted: SparkListenerStageCompleted)={
    completed = true
    completeTime = stageCompleted.stageInfo.completionTime.get
    findSkew()
  }

  private def averageTaskRuntime()={
     tasks.map(_.getDuration()).reduce(_ + _) / tasks.length
  }

  def completeTask(taskEnd: SparkListenerTaskEnd) ={
    tasks(taskEnd.taskInfo.index).end(taskEnd)
  }

  def getName() = name

  private def findSkew(): Boolean = {
    if(tasks.length == 1){
      return false
    }
    if (tasks.length == 2){
      val diff = math.abs(tasks(0).getDuration() - tasks(1).getDuration())

      if(diff > tasks(0).getDuration() * 0.1 || diff > tasks(1).getDuration() * 0.1){
        checkSkewType(diff)
        return true
      }
      return false

    }
    val average = tasks.map(_.getEndTime()).sum / tasks.length
    val std = math.sqrt(tasks.map(task => math.pow(task.getEndTime() - average, 2)).sum / tasks.length)
    for (task <- tasks) {
      if (task.getEndTime() > average + std && std > 10) {
        checkSkewType(task.getEndTime() - average)
        return true
      }
    }
    return false
  }

  private def checkSkewType(timeDiff: Long): Any = {
    if (tasks.length == 2) {
      val readDiff = math.abs(tasks(0).getReadSize() - tasks(1).getReadSize())
      val gcDiff = math.abs(tasks(0).getGcTime() - tasks(1).getGcTime())
      if( readDiff > 10 * 1024){
        println ("Possible data skew in: "+ name)
        println("One task has " + (readDiff/1024) + "KiB more reads comparing to the other")
        println("It is taking %.2fs more than the other".format((timeDiff).toDouble / 1000))
      }
      else if(gcDiff > tasks(0).getGcTime() * 0.1 || gcDiff > tasks(1).getGcTime() * 0.1){
        println("Possible memory skew in: "+ name)
        println("One task is taking %.2fs more on garbage collecting than the other".format(gcDiff.toDouble / 1000))
        println("It is taking %.2fs more in total than the other".format((timeDiff).toDouble / 1000))
      }
      else{
        println("possible computation skew in: " + name)
        println("One task is taking %.2fs more than average".format((timeDiff).toDouble / 1000))
      }

    }
    else{
      val average = tasks.map(_.getReadSize()).sum / tasks.length
      val variance = tasks.map(task => math.pow(task.getReadSize() - average, 2)).sum / tasks.length
      val std = math.sqrt(variance)
      for (task <- tasks) {
        if (task.getReadSize() > average + std) {
          println("Possible data skew in: " + name)
          println("One task has " + (task.getReadSize() - average) / 1024 + "KiB more reads comparing to average")
          println("It is taking %.2fs more than average".format((timeDiff).toDouble / 1000))
          return
        }
      }
      val gcAverage = tasks.map(_.getGcTime()).sum / tasks.length
      val gcVariance = tasks.map(task => math.pow(task.getGcTime() - average, 2)).sum / tasks.length
      val gcStd = math.sqrt(gcVariance)
      for (task <- tasks) {
        if(task.getGcTime() > gcAverage + gcStd){
          println("Possible memory skew in: " + name)
          println("One task is taking %.2fs more on garbage collecting than the average".format((task.getGcTime()-gcAverage).toDouble / 1000))
          println("It is taking %.2fs more in total than average".format((timeDiff).toDouble / 1000))
          return
        }
      }
      println("possible computation skew in: " + name)
      println("One task is taking %.2fs more than average".format((timeDiff).toDouble / 1000))
    }

  }


}

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}

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

  def complete(stageCompleted: SparkListenerStageCompleted)={
    completed = true
    completeTime = stageCompleted.stageInfo.completionTime.get
    println("average task time: " + this.averageTaskRuntime())
    println("stage time:" + (completeTime-startTime))
  }

  private def averageTaskRuntime()={
     tasks.map(_.getDuration()).reduce(_ + _) / tasks.length
  }

}

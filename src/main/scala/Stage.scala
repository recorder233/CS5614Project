import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}

class Stage(){
  private var tasks =Array.empty[Task]
  private var name = ""
  private var id = -1
  private var completed = false
  private var completeTime: Long = -1
  def this(stageSubmitted: SparkListenerStageSubmitted)= {
    this()
    name = stageSubmitted.stageInfo.name
    id = stageSubmitted.stageInfo.stageId
  }

  def addTask(task: Task) ={
    tasks = tasks :+ task
  }

  def complete(stageCompleted: SparkListenerStageCompleted)={
    completed = true
    completeTime = stageCompleted.stageInfo.completionTime.get
  }

}

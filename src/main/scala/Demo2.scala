import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Demo2 extends App{
  Logger.getRootLogger.setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Demo")
    .getOrCreate()

  val skewDetector = new SkewDetector()
  spark.sparkContext.addSparkListener(skewDetector)
  val sc = spark.sparkContext

  //create a RDD with 100 million 1
  val data = spark.sparkContext.parallelize(Seq.fill(100_000)(1))
  data.mapPartitionsWithIndex(mappingRule).collect()

  private def mappingRule(index: Int, part: Iterator[Int]) ={
    if (index == 0) {
      // sleep for 1s to simulate complex computation
      Thread.sleep(1000)
      part
    }
    else {
      part
    }
  }
}

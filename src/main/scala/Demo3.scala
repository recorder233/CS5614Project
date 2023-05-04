import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Demo3 extends App{
  Logger.getRootLogger.setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Demo")
    .getOrCreate()

  val skewDetector = new SkewDetector()
  spark.sparkContext.addSparkListener(skewDetector)
  val sc = spark.sparkContext

  //create a RDD with 100 million 1
  val data = spark.sparkContext.parallelize(Seq.fill(1000)(1))
  val skewedData = data.mapPartitionsWithIndex((index, part) => {
    if (index == 0) {
      //add 100 billion more to first partition
      part ++ Seq.fill(100_000_000)(1)
    }
    else {
      part
    }
  })

  //calculate the sum
  val sum = skewedData.reduce(_ + _)
  println(sum)
}

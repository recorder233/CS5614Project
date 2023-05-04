import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Demo1 extends App{
  Logger.getRootLogger.setLevel(Level.OFF)

  //initiate spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Demo")
    .getOrCreate()

  //add skew detector as a listener
  val skewDetector = new SkewDetector()
  spark.sparkContext.addSparkListener(skewDetector)

  val sc = spark.sparkContext
  //calculate average fare in each fare condition
  val ticket_flights = sc.textFile(("./src/main/data/ticket_flights.csv"))
  val class_price = ticket_flights.map(x => (x.split(",")(2), x.split(",")(3).toInt))
  class_price.groupByKey().mapValues(x => x.sum / x.size).collect().foreach(println)
}

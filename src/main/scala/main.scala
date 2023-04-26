import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger};

object SparkWordCount extends App {
  Logger.getRootLogger.setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val listener = new Listener()
  spark.sparkContext.addSparkListener(listener)

  val sc = spark.sparkContext

  //val ticket_flight = spark.read.format("csv").option("sep", ",").option("header", "true").load("./src/main/data/ticket_flights.csv")
  val ticket_flight = sc.textFile("./src/main/data/ticket_flights.csv")

  val class_price = ticket_flight.map(x => (x.split(",")(2), x.split(",")(3).toInt))
  class_price.groupByKey().mapValues(x => {x.sum / x.size}).collect().foreach(println)
}

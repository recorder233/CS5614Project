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
//
//  //val ticket_flight = spark.read.format("csv").option("sep", ",").option("header", "true").load("./src/main/data/ticket_flights.csv")
//  val ticket_flight = sc.textFile("./src/main/data/ticket_flights.csv")
//
//  val class_price = ticket_flight.map(x => (x.split(",")(2), x.split(",")(3).toInt))
//  class_price.groupByKey().mapValues(x => {x.sum / x.size}).collect().foreach(println)

  val tickets_flights = sc.textFile("./src/main/data/ticket_flights.csv").map(x => x.split(","))
  val flights = sc.textFile("./src/main/data/flights.csv").map(x => x.split(","))
  val aircrafts = sc.textFile(("./src/main/data/aircrafts_data.csv")).map(x=>x.split(","))
  val tickets_aircraft = flights.map(x => (x(7), x))
  val temp = aircrafts.map(x => (x(0), x))
  val joined = tickets_aircraft.join(temp)
  val flights_aircrafts = joined.map(x=>(x._2._1(0), x._2)).persist()
  val tickets_flights_temp = tickets_flights.map(x=>(x(1), x))
  val flights_aircrafts_tickets = flights_aircrafts.join(tickets_flights_temp).map(x =>(x._2._1._1, x._2._1._2, x._2._2)).persist()

  flights_aircrafts_tickets.map(x=>(x._2(1).split(" ")(0), x._3(3).toDouble)).groupByKey().mapValues(stdev).collect().foreach(println)


  private def variance(data: Iterable[Double], ddof:Int = 0): Double =  {
    val n = data.size
    val mean = data.sum / n
    data.map(x => math.pow(x - mean, 2)).sum / (n - ddof)
  }

  def stdev(data:Iterable[Double]):Double = {
    val vari = variance(data)
    val std_dev = math.sqrt(vari)
    std_dev
  }

}

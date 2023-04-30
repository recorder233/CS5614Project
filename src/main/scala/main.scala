import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger};

object main extends App {
  Logger.getRootLogger.setLevel(Level.OFF)
  val spark = SparkSession.builder
    .master("local[*]")
//    .config("spark.executor.instances", 4)
//    .master("yarn-client")
//    .config("spark.executor.memory", "2g")
//    .config("spark.executor.instances", "2")
//    .config("spark.yarn.jars", "hdfs://C:/Users/wangm/AppData/Local/Coursier/cache/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-catalyst_2.13/3.3.2/spark-catalyst_2.13-3.3.2.jar")
    .appName("Spark Word Count")
    .config("spark.ui.killEnabled", "false")
    .getOrCreate()

  val listener = new Listener()
  spark.sparkContext.addSparkListener(listener)

  val sc = spark.sparkContext
  //skewExample1()
  skewExample5()


//
//  //val ticket_flight = spark.read.format("csv").option("sep", ",").option("header", "true").load("./src/main/data/ticket_flights.csv")
//  val ticket_flight = sc.textFile("./src/main/data/ticket_flights.csv")
//
//  val class_price = ticket_flight.map(x => (x.split(",")(2), x.split(",")(3).toInt))
//  class_price.groupByKey().mapValues(x => {x.sum / x.size}).collect().foreach(println)



  val input = scala.io.StdIn.readLine()
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

  def skewExample1()={
    val tickets_flights = sc.textFile("./src/main/data/ticket_flights.csv").map(x => x.split(","))
    val flights = sc.textFile("./src/main/data/flights.csv").map(x => x.split(","))
    val aircrafts = sc.textFile(("./src/main/data/aircrafts_data.csv")).map(x => x.split(","))
    val tickets_aircraft = flights.map(x => (x(7), x))
    val temp = aircrafts.map(x => (x(0), x))
    val joined = tickets_aircraft.join(temp)
    val flights_aircrafts = joined.map(x => (x._2._1(0), x._2)).persist()
    val tickets_flights_temp = tickets_flights.map(x => (x(1), x))
    val flights_aircrafts_tickets = flights_aircrafts.join(tickets_flights_temp).map(x => (x._2._1._1, x._2._1._2, x._2._2)).persist()

    flights_aircrafts_tickets.map(x => (x._2(1).split(" ")(0), x._3(3).toDouble)).groupByKey().mapValues(stdev).collect().foreach(println)
  }

  def skewExample2()={
    val ticket_flights = sc.textFile(("./src/main/data/ticket_flights.csv"))
    val class_price = ticket_flights.map(x =>(x.split(",")(2), x.split(",")(3).toInt))
    class_price.groupByKey().mapValues(x => x.sum / x.size).collect()
  }

  def skewExample3() = {
    val ticket_flights = sc.textFile(("./src/main/data/ticket_flights.csv"))
    var class_price_count = ticket_flights.map(x =>(x.split(",")(2), (x.split(",")(3).toInt, 1)))
    class_price_count = class_price_count.reduceByKey((x, y)=>(x._1 + y._1, x._2 + y._2))
    class_price_count.mapValues(x => x._1 / x._2).collect
  }

  def skewExample4() = {
    val data = spark.sparkContext.parallelize(1 to 1000)
    val squaredRdd = data.map(x => x * x)

    squaredRdd.count()
    val evenRdd = squaredRdd.filter(x => x % 2 == 0)
    val sumResult = evenRdd.sum()
  }

  def skewExample5() = {
    val data = spark.sparkContext.parallelize(Seq.fill(150000000)(1))
    val skewedData = data.mapPartitionsWithIndex((index, part) =>{
      if (index == 0){
        part ++ Seq.fill(150000000)(1)
      }
      else{
        part
      }
    })

    val sum = skewedData.reduce(_ + _)
    println(sum)
  }
}

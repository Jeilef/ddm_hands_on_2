package de.hpi.spark_hands_on

import java.io.File

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class Config(cores: Int = 4,
                  path: String = "./TPCH")

object SparkHandsOn extends App {
  var workers = 4
  var path = "./TPCH"

  override def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("SparkHandsOn") {
      opt[Int]('c', "cores") action { (x, c) =>
        c.copy(cores = x) } text "describes the number of cores that are to be used."

      opt[String]('p', "path") action { (x, c) =>
        c.copy(path = x) } text "describes the path to the directory which contains the data."
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        workers = config.cores
        path = config.path
      case None =>
    }

    println(s"Starting with $workers cores and at path $path...")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$workers]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types

    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    def getDataFiles(path: String):List[String] = {
      val d = new File(path)
      if(d.exists && d.isDirectory) d.listFiles.filter(file =>
        file.isFile &&
          file.getName.split('.').last.contentEquals("csv"))
        .map(_.getPath).toList else List[String]()
    }

    val inputs = getDataFiles(path)

   // println(inputs)
    val reducedInputs = List("region", "nation")
      .map(name => s"./$path/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark)}
  }
}

package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    val datasets = inputs.map(file => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(file))

    //datasets.flatMap(row => row.columns.map(
    //  columnName => (columnName, row.getAs(columnName))

    /*val temp = datasets.head.flatMap(row => {
      val dict = row.getValuesMap(bcNames.value(0))
      dict.toList.map( e => (e._1.toString, e._2.toString))
    })*/
    import spark.implicits._
    val bcNames = spark.sparkContext.broadcast(datasets.map(dataset => dataset.columns))
    //))
    bcNames.value.foreach(_.foreach(println))
    val temp = datasets.head.flatMap(row => bcNames.value(0).map( column => {
      (column.toString, row.getAs(column).toString)
    }))
    temp.show()
    datasets.head.show()


  }
}
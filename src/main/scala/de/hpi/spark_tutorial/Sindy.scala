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
    //))
    val temp = datasets.flatMap(row => row.columns)
    println(temp)



  }
}
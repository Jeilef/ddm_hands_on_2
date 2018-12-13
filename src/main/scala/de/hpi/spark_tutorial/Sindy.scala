package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    val datasets = inputs.map(file => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(file))

    import spark.implicits._
    val bcNames = spark.sparkContext.broadcast(datasets.map(dataset => dataset.columns))

    val longDatasets = bcNames.value.indices.map( index => {
      datasets(index).flatMap(row => bcNames.value(index).map( column => {
        (column.toString, row.getAs(column).toString)
      }))
    })
    longDatasets.foreach(_.show)
  }
}
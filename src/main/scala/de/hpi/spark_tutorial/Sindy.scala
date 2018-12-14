package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val datasets = inputs.map(file => spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(file))

    val bcNames = spark.sparkContext.broadcast(datasets.map(dataset => dataset.columns))

    val longDatasets = bcNames.value.indices.map( index => {
      datasets(index).flatMap(row => bcNames.value(index).map( column => {
        (row.getAs(column).toString, index.toString + "_" + column.toString)
      })).distinct()
    })
      .reduce(_.union(_))
      .persist()

    println("Pair Matching complete...")

    val occurrences = longDatasets
      .map(e => (e._2, 1))
      .rdd.reduceByKey(_+_)
      .toDF
      .withColumnRenamed("_1", "column_name")
      .withColumnRenamed("_2", "value_count")
      .persist()

    println("Counting complete...")

    longDatasets
      .map(e => (e._1, List(e._2)))
      .rdd
      .reduceByKey(_.union(_))
      .flatMapValues(group =>
        group.flatMap( item =>
          group.map(secItem =>
            (item, secItem))
            .filter(entry => entry._1 > entry._2)))
      .map((_, 1))
      .map(row => (row._1._2, row._2))
      .reduceByKey(_+_)
      .map(row => (row._1._1, row._1._2, row._2))
      .toDF
      .join(occurrences, $"_1" === $"column_name")
      .drop("column_name")
      .withColumnRenamed("value_count", "value_count_1")
      .join(occurrences, $"_2" === $"column_name")
      .withColumnRenamed("value_count", "value_count_2")
      .drop("column_name")
      .where("_3 == value_count_1 AND _3 <= value_count_2 OR _3 == value_count_2 AND _3 <= value_count_1")
      .rdd
      .flatMap(row => {
        if(row.getInt(3) < row.getInt(4)) {
          List((row.get(0), row.get(1)))
        }else if(row.getInt(4) < row.getInt(3)){
          List((row.get(1), row.get(0)))
        }else {
          List((row.get(0), row.get(1)), (row.get(1), row.get(0)))
        }
      })
      .map(e => (e._1, List(e._2)))
      .reduceByKey(_.union(_))
      .foreach(e => {
        val foreignKeys = e._2 mkString ", "
        println(e._1 + " < " +  foreignKeys)
      })
    }
}
package fr.publicissapient.pod.dataplateform.csvtoparquet

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object CsvToParquetTest extends App {

  val spark: SparkContext = new SparkContext("local[*]", "glue_app")
  val glueContext: GlueContext = new GlueContext(spark)
  val sparkSession: SparkSession = glueContext.getSparkSession

  val resource = getClass.getResource("") // TODO: make it work

  val csvToParquet = CsvToParquet("", "",
    resource, "my_source", "my_table",
    glueContext, "file://")

  private val schema: StructType = csvToParquet.getSchema

  println(schema)




}

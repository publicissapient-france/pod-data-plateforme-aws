package fr.publicissapient.pod.dataplateform.csvtoparquet

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object CsvToParquetTest extends App {

  val spark: SparkContext = new SparkContext("local[*]", "glue_app")
  val glueContext: GlueContext = new GlueContext(spark)
  val sparkSession: SparkSession = glueContext.getSparkSession

  // val toto = getClass.getResource("")

  val csvToParquet = CsvToParquet("", "",
    "Users/albanphelip/Documents/Xebia/DataPlateforme/pod-data-plateforme-aws/glue/src/test/resources", "my_source", "my_table",
    glueContext, "file://")


  private val schema: StructType = csvToParquet.getSchema

  println(schema)




}

package fr.publicissapient.pod.dataplateform

import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.{GlueArgParser, JsonOptions}
import fr.publicissapient.pod.dataplateform.csvtoparquet.CsvToParquet
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    val parser = ParameterParser(args)

    val csvToParquet = new CsvToParquet(
      rawBucketName = parser.getParameter("raw-bucket-name"),
      preparedBucketName = parser.getParameter("prepared-bucket-name"),
      schemaBucketName = parser.getParameter("schema-bucket-name"),
      sourceName = parser.getParameter("source-name"),
      tableName = parser.getParameter("table-name"),
      glueContext = glueContext
    )

    csvToParquet.run()

  }

  case class ParameterParser(args: Array[String]) {
    def getParameter(parameterName: String): String = {
      GlueArgParser.getResolvedOptions(args, Array(parameterName))(parameterName)
    }
  }
}

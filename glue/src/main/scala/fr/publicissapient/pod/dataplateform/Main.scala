package fr.publicissapient.pod.dataplateform

import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.{GlueArgParser, JsonOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    val glueArgs = GlueArgParser.getResolvedOptions(args, Seq("JOB_NAME").toArray)

    val s3OutputPath = "..."
    val targetDbName = "..."

    val options = JsonOptions(Map("path" -> s3OutputPath, "partitionKeys" -> Seq("region", "year", "month", "day"), "enableUpdateCatalog" -> true))

    val df = glueContext.read.table("...")

    val dynamicFrame = DynamicFrame(df, glueContext)
    val sink = glueContext.getCatalogSink(database = targetDbName, tableName = targetDbName, additionalOptions = options)
    sink.writeDynamicFrame(dynamicFrame)


  }




}

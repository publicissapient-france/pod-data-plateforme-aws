package fr.publicissapient.pod.dataplateform.csvtoparquet

import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.Source
import scala.util.parsing.json.JSON

class CsvToParquet(rawBucketName: String,
                   preparedBucketName: String,
                   schemaBucketName: String,
                   sourceName: String,
                   tableName: String,
                   glueContext: GlueContext
                  ) {

  val rawPath = s"s3://$rawBucketName/data/$sourceName/$tableName"
  val preparedPath = s"s3://$preparedBucketName/data/$sourceName/$tableName"
  val schemaPath = s"s3://$schemaBucketName/schema/$sourceName/$tableName.json"

  def getSchema: StructType = {
    val readSchemaJson= glueContext.sparkContext.textFile(schemaPath).collect().mkString
    // val readSchemaJson= Source.fromFile("schema.json").getLines.mkString

    val map:Map[String,Any] = JSON.parseFull(readSchemaJson).get.asInstanceOf[Map[String, Any]]
    val schemaString: String = map("fields").asInstanceOf[List[Any]].map(_.toString).mkString("[", ",", "]")

    DataType.fromJson(schemaString).asInstanceOf[StructType]
  }

  def run(): Unit = {

    val df = glueContext.read
      .schema(getSchema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .csv(rawPath)

    val dynamicFrame = DynamicFrame(df, glueContext)

    val outputOptions = Map(
      "path" -> preparedPath,
      // "partitionKeys" -> Seq("region", "year", "month", "day"),
      "enableUpdateCatalog" -> true
    )

    val sink = glueContext.getCatalogSink(database = sourceName, tableName = tableName, additionalOptions = JsonOptions(outputOptions))
    sink.writeDynamicFrame(dynamicFrame)
  }

}

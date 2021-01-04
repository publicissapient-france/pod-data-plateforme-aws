package fr.publicissapient.pod.dataplateform.csvtoparquet

import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.types.{DataType, StructType}

case class CsvToParquet(rawBucketName: String,
                        preparedBucketName: String,
                        schemaBucketName: String,
                        sourceName: String,
                        tableName: String,
                        glueContext: GlueContext,
                        fileSystem: String = "s3:/"
                       ) {

  val rawPath = s"$fileSystem/$rawBucketName/data/$sourceName/$tableName"
  val preparedPath = s"$fileSystem/$preparedBucketName/data/$sourceName/$tableName"
  val schemaPath = s"$fileSystem/$schemaBucketName/schema/$sourceName/$tableName.json"

  def getSchema: StructType = {
    val readSchemaJson = glueContext.sparkContext.textFile(schemaPath).collect().mkString
    DataType.fromJson(readSchemaJson).asInstanceOf[StructType]
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

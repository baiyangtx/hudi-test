import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.HoodieJavaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.example.Descriptor

import java.io.File

case class HudiJob(spark: SparkSession, warehouse: File) {

  private def sql(sqlText: String): Unit = {
    System.out.print(sqlText)
    spark.sql(sqlText).show()

  }


  def execute(): Unit = {
    sql(
      s"""
        |CREATE TABLE test (
        | id int  NOT NULL COMMENT 'Hoodie Key',
        | data string,
        | pt string
        | ) using hudi
        | partitioned by (pt)
        | tblproperties (
        |   'hoodie.amoro.optimizing.auto.enabled' = 'true'
        | )
        |""".stripMargin)
    val catalog = spark.sessionState.catalogManager
      .currentCatalog.asInstanceOf[TableCatalog]
    val table = catalog.loadTable(Identifier.of(Array("default"), "test"))
    println(table)
    val location = table.properties().get("location")
    println(table.properties())

    val engineContext = new HoodieJavaEngineContext(new Configuration())
    val config = HoodieWriteConfig.newBuilder()
      .withPath(location)
      .build()
    val hoodieTable = HoodieJavaTable.create(
      config,
      engineContext
    )

    val tableConfig = hoodieTable.getMetaClient.getTableConfig

    println("Hoodie table config", tableConfig)

    val descriptor = new Descriptor(hoodieTable)
    descriptor.toSchema();

    val cols = Seq("id", "data", "pt")
    val data = Seq(
      (1, "aaa", "20200401"),
      (2, "bbb", "20200402"),
      (3, "ccc", "20200403")
    )
    val insert = spark.createDataFrame(data).toDF(cols: _*)
    insert.createOrReplaceTempView("source")
    sql("insert into test select * from source")




  }
}

object HudiJob {

  def main(args: Array[String]): Unit = {
    val warehouse = new File("SparkWarehouse")
    FileUtils.deleteQuietly(warehouse)
    warehouse.mkdirs()
    val spark = SparkSession.builder
      .appName("Hudi job")
      .config("spark.sql.warehouse.dir", warehouse.getAbsoluteFile.getPath)
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .master("local[*]")
      .getOrCreate()

    val job = HudiJob(spark, warehouse.getAbsoluteFile)
    job.execute()
  }

}

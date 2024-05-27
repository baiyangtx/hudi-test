import org.apache.hadoop.conf.Configuration
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.HoodieJavaTable

import java.io.File

object HoodieInspect {
  def main(args: Array[String]): Unit = {
    val location = new File("SparkWarehouse/").getAbsoluteFile.toString
    val config = HoodieWriteConfig.newBuilder()
      .withPath(location)
      .build()
    val engineContext = new HoodieJavaEngineContext(new Configuration())
    val hoodieTable = HoodieJavaTable.create(
      config,
      engineContext
    )
  }
}

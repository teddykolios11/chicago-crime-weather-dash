import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

val hiveTable = "tkolios_crime_feature_importance"

// read table from hive
val df = spark.table(hiveTable)

val tableName = "tkolios_crime_feature_importance"

val conf = HBaseConfiguration.create()
val connection = ConnectionFactory.createConnection(conf)
val table = connection.getTable(TableName.valueOf(tableName))

val rowKey = Bytes.toBytes("model_v1")
val put = new Put(rowKey)

df.collect().foreach { row =>
  val feature = row.getAs[String]("feature")
  val importance = row.getAs[Double]("importance")
  put.addColumn(
    Bytes.toBytes("imp"),
    Bytes.toBytes(feature),                    
    Bytes.toBytes(importance)
  )
}

table.put(put)
table.close()
connection.close()
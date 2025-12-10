import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

val hiveTable = "tkolios_crime_weather_hourly_scala"

// read table from hive
val df = spark.table(hiveTable)

// compute average crime_count by hour over all days
val hourlyAvg = df.groupBy("hour").agg(avg("crime_count").as("avg_crime_count"))

val rows = hourlyAvg.collect()

val tableName = "tkolios_crime_by_hour"
val cf       = Bytes.toBytes("count")

val conf = HBaseConfiguration.create()
val connection = ConnectionFactory.createConnection(conf)
val table = connection.getTable(TableName.valueOf(tableName))

try {
  val rowKey = Bytes.toBytes("ALL")
  val put = new Put(rowKey)

  rows.foreach { row =>
    val hour        = row.getAs[Int]("hour")
    val crimeCount  = row.getAs[Long]("crime_count")
    val qual        = Bytes.toBytes(hour.toString)

    put.addColumn(cf, qual, Bytes.toBytes(crimeCount))
  }

  table.put(put)
} finally {
  table.close()
  connection.close()
}
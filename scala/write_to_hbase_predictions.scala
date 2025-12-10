import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

val hiveTable = "tkolios_crime_predictor_grid"

// read table from hive
val preds = spark.table(hiveTable)

val tableName = "tkolios_crime_predictor"

val confHb = HBaseConfiguration.create()
val connection = ConnectionFactory.createConnection(confHb)
val table = connection.getTable(TableName.valueOf(tableName))

preds.collect().foreach { row =>
    val hour       = row.getAs[Int]("hour")
    val dow        = row.getAs[Int]("day_of_week")
    val tmax       = row.getAs[Double]("tmax")
    val tmin       = row.getAs[Double]("tmin")
    val prcp       = row.getAs[Double]("prcp")
    val snow       = row.getAs[Double]("snow")
    val snwd       = row.getAs[Double]("snwd")
    val prediction = row.getAs[Double]("prediction")

    // convert to ints
    val tmaxI = tmax.toInt
    val prcpI = prcp.toInt
    val snowI = snow.toInt
    val snwdI = snwd.toInt

    val rowKey = s"${dow}_${hour}_${tmaxI}_${prcpI}_${snowI}_${snwdI}"

    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("y_hat"), Bytes.toBytes(prediction))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("hour"), Bytes.toBytes(hour))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("day_of_week"), Bytes.toBytes(dow))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("tmax"), Bytes.toBytes(tmax))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("prcp"), Bytes.toBytes(prcp))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("snow"), Bytes.toBytes(snow))
    put.addColumn(Bytes.toBytes("pred"), Bytes.toBytes("snwd"), Bytes.toBytes(snwd))

    table.put(put)
  }

table.close()
connection.close()

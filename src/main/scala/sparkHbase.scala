import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put

/**
  * Created by geci on 31/05/2016.
  */
object sparkHbase {


  def main(args: Array[String]) {
    getDataFrameAPIHadoopRDD()
    getDataFrameHBaseContext()

    insertDataHBaseContext()
  }

  def getDataFrameAPIHadoopRDD(): Unit = {
    val sc = new SparkContext()

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import sqlContext.implicits._

    val conf = HBaseConfiguration.create()
    val tableName = "employee"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val cols = hBaseRDD.map(tuple => tuple._2).map(result => result.getColumn("personal data".getBytes, "city".getBytes) :: result.getColumn("personal data".getBytes, "name".getBytes) :: result.getColumn("professional data".getBytes, "designation".getBytes) :: result.getColumn("professional data".getBytes, "salary".getBytes) :: Nil)

    case class Employee(city: String, name: String, designation: String, salary: Int)

    val filtered = cols.filter(row => row.map(_.size > 0).reduce((acc, tip) => acc & tip))

    val employee = filtered.map(res => Employee(new String(res(0).get(0).getValue.map(_.toChar)), new String(res(1).get(0).getValue.map(_.toChar)), new String(res(2).get(0).getValue.map(_.toChar)), new String(res(3).get(0).getValue.map(_.toChar)).toInt))

    //create data frame
    employee.toDF()
  }

  def getDataFrameHBaseContext(): Unit = {
    val sc = new SparkContext()

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import sqlContext.implicits._

    val conf = HBaseConfiguration.create()
    val tableName = "employee"
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "personal data")
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "professional data")

    val hbaseContext = new HBaseContext(sc, conf)
    var scan = new Scan()
    scan.addFamily(Bytes.toBytes("personal data"))
    scan.addFamily(Bytes.toBytes("professional data"))

    case class Employee(city: String, name: String, designation: String, salary: Int)

    val rddScan = hbaseContext.hbaseScanRDD(tableName, scan)

    val dataFrame = rddScan.map(tuple => tuple._2).map(values => Employee( new String(values.get(0)._3.map(_.toChar)), new String(values.get(1)._3.map(_.toChar)),new String(values.get(2)._3.map(_.toChar)),new String(values.get(3)._3.map(_.toChar)).toInt ) ).toDF()
  }

  def insertDataHBaseContext(): Unit = {
    val sc = new SparkContext()

    val conf = HBaseConfiguration.create()
    val tableName = "employee"
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val hbaseContext = new HBaseContext(sc, conf)

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("2"), Array((Bytes.toBytes("personal data"), Bytes.toBytes("city"), Bytes.toBytes("votuporanga")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes("personal data"), Bytes.toBytes("name"), Bytes.toBytes("Guilherme")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes("professional data"), Bytes.toBytes("designation"), Bytes.toBytes("developer")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes("professional data"), Bytes.toBytes("salary"), Bytes.toBytes("100000"))))
    ))

    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd, tableName,
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
        put
      }, true)
  }


}

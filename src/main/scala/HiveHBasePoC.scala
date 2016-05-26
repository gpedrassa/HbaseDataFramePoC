import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HiveHBasePoC {
  def main(args: Array[String]) {

    val sc = new SparkContext()

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //create HIVE table and load data
    sqlContext.sql("DROP TABLE IF EXISTS employee")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("LOAD DATA LOCAL INPATH 'test.txt' OVERWRITE INTO TABLE employee")

    // create DataFrame HIVE
    val resultHive = sqlContext.sql("FROM employee SELECT *")

    println("Hive count "+resultHive.count)

    // HBase
    val conf = HBaseConfiguration.create()
    val tableName = "hive-hbase-integration"
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    // create HBase RDD
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println("Counnt RDD Hbase "+hBaseRDD.count())
  }


}

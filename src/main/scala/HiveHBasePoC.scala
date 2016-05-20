import org.apache.spark.{SparkConf, SparkContext}

object HiveHBasePoC {
  def main(args: Array[String]) {
    val sc = new SparkContext()

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("DROP TABLE IF EXISTS employee")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")

  }
}

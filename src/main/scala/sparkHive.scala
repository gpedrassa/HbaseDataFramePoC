import org.apache.spark.SparkContext

/**
  * Created by geci on 31/05/2016.
  */
object sparkHive {
  def main(args: Array[String]) {
    val sc = new SparkContext()

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //create HIVE table and load data
    sqlContext.sql("DROP TABLE IF EXISTS employee")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    sqlContext.sql("LOAD DATA LOCAL INPATH 'test.txt' OVERWRITE INTO TABLE employee")

    // create DataFrame
    val resultSqlHive = sqlContext.sql("FROM employee SELECT *")

    // also can be created using
    val resultTableHive = sqlContext.table("employee")

    System.out.println("SQL Result Employee: "+resultSqlHive.count)
    System.out.println("Table Result Employee: "+resultTableHive.count)

  }
}

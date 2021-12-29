import org.apache.spark.sql.SparkSession

object hivetest5 {

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    //spark.sparkContext.setLogLevel("ERROR")
    spark.sql("create table IF NOT EXISTS newone(id Int,name String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE newone")
    spark.sql("SELECT * FROM newone").show()
    spark.close()
  }
}

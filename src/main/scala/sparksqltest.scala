import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object sparksqltest {
  def main(args:Array[String]):Unit = {
    /*
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setMaster("local").setAppName("hello hive")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

     */
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    //spark.sql("use default")
    spark.sql("show databases").show()
    spark.sql("show tables from default").show()
    spark.close()
  }
}

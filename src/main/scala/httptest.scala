import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params.HttpMethodParams
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import java.nio.file.{Paths, Files}

import scala.io.Source.fromURL

object httptest {
  def main(args:Array[String]):Unit= {
    /*
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
     */
    val conf = new SparkConf().setMaster("local").setAppName("hello hive")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val games_test = scala.io.Source.fromURL("https://www.balldontlie.io/api/v1/games").mkString
    val gameRdd = sc.parallelize(Seq(games_test))
    if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\games.json"))) {
      gameRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\games.json")
    }
    val games_json = spark.read.json("C:\\Users\\benja\\Desktop\\Revature\\games.json")
    //games_json.select(explode($"data").as("exploded")).select("exploded.*")
    games_json.select(explode(col("data")).as("exploded")).select("exploded.*").show
    spark.close()
  }
}

/*
object httptest {
  def main(args:Array[String]):Unit= {
    // Create an instance of HttpClient.
    //var url:String = "http://www.apache.org/"
    var url:String = "https://www.balldontlie.io/api/v1/teams/14"
    var client:HttpClient = new HttpClient();

    // Create a method instance.
    var method:GetMethod = new GetMethod(url);

    // Provide custom retry handler is necessary
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
      new DefaultHttpMethodRetryHandler(3, false));

    try {
      // Execute the method.
      var statusCode:Int = client.executeMethod(method);

      if (statusCode != HttpStatus.SC_OK) {
        System.err.println("Method failed: " + method.getStatusLine());
      }

      // Read the response body.
      var responseBody:Array[Byte] = method.getResponseBody();

      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary data
      System.out.println(new String(responseBody));

    } catch  {
      case e: HttpException => {
        System.err.println("Fatal protocol cviolation: " + e.getMessage())
        e.printStackTrace()
      }
      //case e: IOException => e.printStackTrace()
      //System.err.println("Fatal transport error: " + e.getMessage());
      //e.printStackTrace();
    } finally {
      // Release the connection.
      method.releaseConnection();
    }
  }
}*/

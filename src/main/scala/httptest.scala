import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params.HttpMethodParams
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, explode, from_json, lit, max}

import java.nio.file.{Files, Paths}
import scala.io.StdIn.readLine
import scala.io.Source.fromURL

object httptest {
  def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))

  //val game_jsonduh = ???

  def main(args:Array[String]):Unit= {
    /*
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
     */

    /*
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

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    //spark.sql("use defualt")
    spark.sql("show tables").show()
    var admin_key = "hu2083h34"
    var admin_user = 2
    var basic_user = 1
    var user_access = 0
    //run the below code to setup admin account
    spark.sql("create table if not exists Users(username String, password  String, privilege Int)")
    //spark.sql("insert into table Users values (\"ben_ruiz\",\"password123\",2)")
    spark.sql("select * from Users").show
    //
    var exiting = false
    while (!exiting) {
      var loggedin = false
      var signedup = false
      var user_name = ""
      var user_password = ""
      var privilege = 0
      while (!loggedin) {
        println("What would you like to do?")
        println("Login (L)")
        println("Sign up (S)")
        println("Exit (E)")
        val user_input = readLine()
        if (user_input == "L") {
          println("Welcome! What is your username? Enter exit if you would like to quit")
          val username = readLine()
          if (username != "exit") {
            println("What is your password?")
            val password = readLine()
            println(username + " " + password)
            val user_privilege = spark.sql(s"select privilege from Users where Users.username=\'$username\'")
            //val privilege = user_privilege.first()
            if (!user_privilege.head(1).isEmpty) {
              privilege = user_privilege.collect()(0).getInt(0)
              user_name = username
              user_password = password
              user_access = privilege
              //println(user_privilege.first())
              println(privilege)
              loggedin = true
              println("user found and logged in")
            }
            else {
              println("The user doesn't exist")
              //  loggedin = true
              //  exiting = true
            }
          }
        }
        else if (user_input == "S") {
          while (!signedup) {
            var valid_username = false
            var valid_password = false
            var desired_username = ""
            var desired_password = ""
            println("What would you like your username to be? Enter Exit if you would like to quit")
            val username1 = readLine()
            if (username1 == "Exit") {
              signedup = true
              exiting = true
            }
            else {
              println("Please confirm your username")
              val username2 = readLine()
              if (username1 == username2) {
                val taken_username = spark.sql(s"select username from Users where Users.username=\'$username1\'")
                //val privilege = user_privilege.first()
                if (!taken_username.head(1).isEmpty) {
                  println("That username is taken, try another one")
                }
                else {
                  println("The username is valid")
                  valid_username = true
                  desired_username = username1
                  //  loggedin = true
                  //  exiting = true
                }
              }
            }
            if (valid_username) {
              var password1 = "a"
              var password2 = "b"
              while (password1 != password2) {
                println("What would you like your password to be?")
                password1 = readLine()
                println("please confirm your password")
                password2 = readLine()
                if (password1 != password2) {
                  println("Passwords did not match. Try again")
                }
              }
              desired_password = password1
              valid_password = true
            }
            if (valid_password) {
              while (user_access == 0) {
                println("Would you like an admin account or basic account?")
                println("Enter Admin or Basic")
                val input = readLine()
                if (input == "Admin") {
                  println("What is the admin key??")
                  val input_key = readLine()
                  if (input_key == admin_key) {
                    spark.sql(s"insert into table Users values (\'$desired_username\',\'$desired_password\',2)")
                    signedup = true
                    //user_name = desired_username
                    //user_password = desired_password
                    user_access = 2
                    println("Admin account made")
                  }
                  else {
                    println("Invalid admin key entered")
                  }
                }
                else if (input == "Basic") {
                  spark.sql(s"insert into table Users values (\'$desired_username\',\'$desired_password\',1)")
                  signedup = true
                  //user_name = desired_username
                  //user_password = desired_password
                  user_access = 1
                  println("Basic user account made")
                }
              }
              user_access = 0
            }
          }
          signedup = false
        }
        else if (user_input == "E") {
          loggedin = true
          exiting = true
        }
        else {
          println("Invalid input, try again")
        }
      }
      if (exiting) {
        loggedin = false
        signedup = false
      }
      if (loggedin) {
        println("What would you like to do?")
        println("Update password (U)")
        println("Delete account (D)")
        println("Run Queries (R)")
        val user_command = readLine()
        if (user_command == "U") {
          var attempt1 = "a"
          var attemp2 = "b"
          while (attempt1 != attemp2) {
            println("What would you like your new password to be?")
            attempt1 = readLine()
            println("Please confirm new password")
            attemp2 = readLine()
          }
          val remaining_users = spark.sql(s"select * from Users where Users.username!=\'$user_name\'")
          remaining_users.registerTempTable("tempusers")
          spark.sql("drop table if exists tempusers2")
          spark.sql("create table tempusers2 as select * from tempusers")
          //spark.sql("select * from tempusers2").show
          spark.sql("drop table Users")
          spark.sql("ALTER TABLE tempusers2 RENAME TO Users")
          spark.sql(s"insert into table Users values (\'$user_name\',\'$attempt1\',$user_access)")

        }
        else if (user_command == "D") {
          println("Are you sure? Enter your username if you are sure")
          val delete_request = readLine()
          if (delete_request == user_name) {
            val remaining_users = spark.sql(s"select * from Users where Users.username!=\'$user_name\'")
            remaining_users.registerTempTable("tempusers")
            //spark.sql("select * from tempusers").show
            //spark.sql("select * from tempusers").show
            spark.sql("drop table if exists tempusers2")
            spark.sql("create table tempusers2 as select * from tempusers")
            //spark.sql("select * from tempusers2").show
            spark.sql("drop table Users")
            spark.sql("ALTER TABLE tempusers2 RENAME TO Users")


            println("Your account has been deleted")
          }
          else {
            println("Your account has not been deleted")
          }
        }
        else if (user_command == "R") {
          //val login_input = readLine()
          var games_test = scala.io.Source.fromURL("https://www.balldontlie.io/api/v1/games?per_page=100&seasons[]=2019").mkString
          var gameRdd = sc.parallelize(Seq(games_test))
          if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\games.json"))) {
            gameRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\games.json")
          }
          var games_json = spark.read.json("C:\\Users\\benja\\Desktop\\Revature\\games.json")
          var games_table = games_json.select(explode(col("data")).as("exploded")).select("exploded.*")
          //games_table.registerTempTable("games")//This line creates a temporary table so we can issue queries
          //using HiveQL language instead of using scala functions
          spark.sql("drop table if exists games")
          games_table.write.saveAsTable("games") //This line creates a permanent table

          games_test = scala.io.Source.fromURL("https://www.balldontlie.io/api/v1/games?per_page=100&seasons[]=2019&page=2").mkString
          gameRdd = sc.parallelize(Seq(games_test))
          if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\games2.json"))) {
            gameRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\games2.json")
          }
          games_json = spark.read.json("C:\\Users\\benja\\Desktop\\Revature\\games2.json")
          games_table = games_json.select(explode(col("data")).as("exploded")).select("exploded.*")
          games_table.write.mode("append").saveAsTable("games")
          //games_json.select(explode($"data").as("exploded")).select("exploded.*")
          //games_json.select(explode(col("data")).as("exploded")).select("exploded.*").show
          //games_json.select(explode(col("data")).as("exploded")).select(max("exploded.home_team_score")-max("exploded.visitor_team_score")).show
          //games_json.select(explode(col("data")).as("exploded")).select("exploded.home_team_score").show
          //spark.sql("select * from games").show
          println("What is the max difference in scores between teams in games played in 2019?")
          //spark.sql("select home_team,visitor_team,(max(abs(home_team_score-visitor_team_score))) as max_score from games group by home_team,visitor_team").show
          val temp_table = spark.sql("select home_team,visitor_team,(max(abs(home_team_score-visitor_team_score))) as abs_score from games group by id,home_team,visitor_team")
          temp_table.registerTempTable("absolute_scores")
          spark.sql("select max(abs_score) from absolute_scores").show
          println("What is the min difference in score between teams in games played in 2019")
          spark.sql("select min(abs_score) from absolute_scores").show
          println("What is the average difference in score between teams in games played in 2019")
          spark.sql("select avg(abs_score) from absolute_scores").show

          //spark.sql("select home_team,visitor_team,(max(abs(home_team_score-visitor_team_score))) as max_score from games order by max(abs(home_team_score-visitor_team_score))").show
          /*
        val players_test = scala.io.Source.fromURL("https://www.balldontlie.io/api/v1/players?per_page=100").mkString
        val playerRdd = sc.parallelize(Seq(players_test))
        if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\players.json"))) {
          playerRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\players.json")
        }
        val players_json = spark.read.json("C:\\Users\\benja\\Desktop\\Revature\\players.json")
        val players_table = players_json.select(explode(col("data")).as("exploded")).select("exploded.*")
        spark.sql("drop table if exists players")
        players_table.write.saveAsTable("players")
        println("Some stuff about players in the 2019 season")
        spark.sql("select * from players").show
        */

          var stats_test = scala.io.Source.fromURL("https://www.balldontlie.io/api/v1/stats?per_page=100&page=1").mkString
          var statsRdd = sc.parallelize(Seq(stats_test))
          if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\stats.json"))) {
            statsRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\stats.json")
          }
          var stats_json = spark.read.json("C:\\Users\\benja\\Desktop\\Revature\\stats.json")
          var stats_table = stats_json.select(explode(col("data")).as("exploded")).select("exploded.*")
          spark.sql("drop table if exists stats")
          stats_table.write.saveAsTable("stats")

          stats_test = scala.io.Source.fromURL("https://www.balldontlie.io/api/v1/stats?per_page=100&page=2").mkString
          statsRdd = sc.parallelize(Seq(stats_test))
          if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\stats2.json"))) {
            statsRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\stats2.json")
          }
          stats_json = spark.read.json("C:\\Users\\benja\\Desktop\\Revature\\stats2.json")
          stats_table = stats_json.select(explode(col("data")).as("exploded")).select("exploded.*")
          stats_table.write.mode("append").saveAsTable("stats")


          if (!Files.exists(Paths.get("C:\\Users\\benja\\Desktop\\Revature\\players.json"))) {
            statsRdd.saveAsTextFile("C:\\Users\\benja\\Desktop\\Revature\\players.json")
          }

          //println("Some stuff about player stats")
          //spark.sql("select * from stats").show

          var players_table = stats_table.select(col("player.*"))
          spark.sql("drop table if exists players")
          players_table.write.saveAsTable("players")
          var center_results = spark.sql("select ((height_feet*12)+height_inches) as height,position from players where position=\'C\'")
          center_results.registerTempTable("centerheights")
          println("What is the max height of centers in 2019")
          spark.sql("select max(height) from centerheights").show
          println("What is the min height of centers in 2019")
          spark.sql("select min(height) from centerheights").show
          println("What is the average height of centers in 2019")
          spark.sql("select avg(height) from centerheights").show
          //var players_table = spark.sql("select player from stats")
          //players_table.show
        }

        //spark.sql("show tables").show()
      }
    }
    spark.close()
  }
  //"C:\\Users\\benja\\Desktop\\Revature\\games_csv"
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

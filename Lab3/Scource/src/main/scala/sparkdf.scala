
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object sparkdf {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    val df1 = spark.read.format("csv").option("header", "true").load("F:\\spark\\Lab3\\Scource\\src\\main\\scala\\data_sets_worldcup\\WorldCupMatches.csv")

    val df2 = spark.read.format("csv").option("header", "true").load("F:\\spark\\Lab3\\Scource\\src\\main\\scala\\data_sets_worldcup\\worldcups.csv")

    df1.createGlobalTempView("WorldCupMatches")
    df2.createGlobalTempView("worldcups")
    // Global temporary view is tied to a system preserved database `global_temp`

    spark.sql("SELECT Stage, Stadium, City, Attendance, MatchID FROM global_temp.WorldCupMatches").show(20)


    val sqlDF1 = spark.sql("SELECT Stage, Stadium, City from global_temp.WorldCupMatches")
    sqlDF1.show(20)
    val sqlDF2 = spark.sql("SELECT year, Country, Winner FROM global_temp.worldcups")
    sqlDF2.show()
    val unionDF = sqlDF1.union(sqlDF2)
    unionDF.show()
    val y = spark.sql("SELECT Year, Country, GoalsScored From global_temp.worldcups WHERE GoalsScored>100")
    y.show(10)
    val Pattern_reg = spark.sql("SELECT * from global_temp.WorldCupMatches WHERE Stage LIKE 'Final' AND Home_Team_Initials LIKE 'ENG'")
    Pattern_reg.show(10)
    val Avg_goals = spark.sql("SELECT Home_Team_Name AS Team, ROUND(AVG(Home_Team_Goals),0) AS average_goals FROM global_temp.WorldCupMatches GROUP BY Home_TEam_Name")
    Avg_goals.show(10)
    //RDD
    //val RDD = spark.sparkContext.textFile("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\LAB ASSIGNMENT3\\WorldCupPlayers_t")
    val Goals = spark.sql("SELECT Stage,Stadium,City,Home_Team_Name FROM global_temp.WorldCupMatches WHERE Home_Team_Goals >6")
    Goals.show(10)
    df1.select("Stadium").show(5)
    df1.groupBy("City").count().show(5)



    //rdd
    val wc_rdd=df2.rdd
    print(wc_rdd.count())
    //wc_rdd.
//print(wc_rdd.filter(col("Country").like("Italy")).count())
   // print(wc_rdd.map(x=> x(8)).sum())

  }
}

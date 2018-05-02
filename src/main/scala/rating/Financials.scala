package rating
import java.util

import org.apache.spark.sql.DataFrame
import running.MainApp.spark
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory.load
import scala.collection.mutable.ListBuffer
import collection.JavaConversions._
import org.apache.spark.sql.Row
import running.MainApp.mainDF

class Financials {
  val sysProperties = System.getProperties()
  val df = mainDF

  def loopColsMicro(cols: String): List[List[String]] ={
    var pointsMicroTmp: String = null
    var pointsListTmp = new ListBuffer[Any]
    var colListTmp = new ListBuffer[String]()
    var finalListTmp = new ListBuffer[Any]

    load("application.conf").getConfig("app").entrySet().foreach { entry =>
      if (entry.getKey.matches("(.*)" + cols + "(.*)")) {
        colListTmp += entry.getValue.unwrapped().toString
        pointsMicroTmp = entry.getKey.replace("."+ cols,".points_micro")
          load("application.conf").getConfig("app").entrySet().foreach {points =>
            if(points.getKey() == pointsMicroTmp){
              pointsListTmp += points.getValue.unwrapped()
            }
        }
      }
    }
    val pointsList = pointsListTmp.map(_.asInstanceOf[util.ArrayList[Int]])
    val colListTmpSorted = colListTmp.toList.sorted
    for (x <- 0 to colListTmpSorted.length - 1) {
      finalListTmp += List(colListTmpSorted(x),pointsList(x)(0),pointsList(x)(1),pointsList(x)(2))
    }

    val finalList = finalListTmp.toList.map(_.asInstanceOf[List[String]])
    finalList
  }

  def loopColsOther(cols: String): List[List[String]] ={
    var pointsMicroTmp: String = null
    var pointsListTmp = new ListBuffer[Any]
    var colListTmp = new ListBuffer[String]()
    var finalListTmp = new ListBuffer[Any]

    load("application.conf").getConfig("app").entrySet().foreach { entry =>
      if (entry.getKey.matches("(.*)" + cols + "(.*)")) {
        colListTmp += entry.getValue.unwrapped().toString
        pointsMicroTmp = entry.getKey.replace("."+ "main_column_name",".points_other")
        load("application.conf").getConfig("app").entrySet().foreach {points =>
          if(points.getKey() == pointsMicroTmp){
            pointsListTmp += points.getValue.unwrapped()
          }
        }
      }
    }

    val pointsList = pointsListTmp.map(_.asInstanceOf[util.ArrayList[Int]])
    val colListTmpSorted = colListTmp.toList.sorted
    for (x <- 0 to colListTmpSorted.length - 1) {
      finalListTmp += List(colListTmpSorted(x),pointsList(x)(0),pointsList(x)(1),pointsList(x)(2))
    }

    val finalList = finalListTmp.toList.map(_.asInstanceOf[List[String]])
    finalList
  }

  def financialsMicro(curr_col_name: List[List[String]], prev_col_name: List[List[String]]): DataFrame ={
    var finalDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],df.schema).withColumn("points",lit(null))

      for (x <- 0 to curr_col_name.length -1 ){
        var returnDF = df.withColumn("points", when(col(curr_col_name(x)(0)) - col(prev_col_name(x)(0)) > 0,curr_col_name(x)(1))
          .when(col(curr_col_name(x)(0)) - col(prev_col_name(x)(0)) === 0 || (col(curr_col_name(x)(0)) - col(prev_col_name(x)(0))).isNull, curr_col_name(x)(2))
          .when(col(curr_col_name(x)(0)) - col(prev_col_name(x)(0)) < 0, curr_col_name(x)(3)))
        finalDF = finalDF.union(returnDF)
      }
    finalDF
  }

  def financialsOther(colName: List[List[String]]): DataFrame ={
    var finalDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],df.schema).withColumn("points",lit(null))

    for (x <- 0 to colName.length -1 ){
      var returnDF = df.withColumn("points", when(col(colName(x)(0)) > 0,colName(x)(1))
        .when(col(colName(x)(0)) === 0 || col(colName(x)(0)).isNull, colName(x)(2))
        .when(col(colName(x)(0))  < 0, colName(x)(3)))
      finalDF = finalDF.union(returnDF)
    }
    finalDF
  }
}

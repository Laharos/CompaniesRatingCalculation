package running
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import rating.{Companies, Financials}

object MainApp extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions","50").appName("Scala")/*.master("local[*]")*/.getOrCreate()

  import spark.implicits._
  val mainDF: DataFrame = new DataRead().getMainDF.cache()

  val c: Companies = new Companies()
  val finMicroCurrCols: List[List[String]] = new Financials().loopColsMicro("current_column_name")
  val finMicroPrevCols: List[List[String]] = new Financials().loopColsMicro("previous_column_name")
  val finOtherCols: List[List[String]] = new Financials().loopColsOther("financials_others(.*)main_column_name")

   val finMicro: DataFrame = new Financials().
    financialsMicro(finMicroCurrCols,finMicroPrevCols).
    select($"companynumber",col("points")).groupBy($"companynumber").agg(sum("points") as "points")

  val finOthers: DataFrame = new Financials().
    financialsOther(finOtherCols).
    select($"companynumber",col("points")).groupBy($"companynumber").agg(sum("points") as "points")

  val financials: DataFrame = finMicro.union(finOthers).
    groupBy($"companynumber").agg(sum("points") as "points")

  val companies: DataFrame = c.companiesCountryOfOrigin.
    union(c.companiesStatus).
    union(c.companiesCategory).
    union(c.companiesAge).
    union(c.companiesAccCategory).
    union(c.companiesSicCode).
    union(c.companiesPostTown).
    select($"companynumber",col("points")).groupBy($"companynumber").agg(sum("points") as "points")

  val finalOutput: DataFrame = financials.union(companies).select($"companynumber",col("points")).groupBy($"companynumber").agg(sum("points") as "points")

  new DataSave().saveDataFrame(finalOutput,"cf_rating.final_rating")

  spark.stop()
}

package rating

import java.util
import java.util.List

import com.typesafe.config.ConfigFactory.load
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lower, when}
import running.DataRead
import org.apache.spark.sql.functions._
import running.MainApp.mainDF

import collection.JavaConversions._
import running.MainApp.spark.implicits._

class Companies {
  var category_column_name: String = null
  var tier_1: List[String] = null
  var tier_2: List[String] = null
  var tier_3: List[String] = null
  var tier_4: List[String] = null
  var tier_5: List[String] = null
  var tier_6: List[String] = null
  var tier_points: List[Integer] = null

  val df = mainDF

  def companiesCountryOfOrigin: DataFrame = {
    category_column_name = load().getString("app.rating_conf.company.country_origin.main_column_name")
    tier_1 = load().getStringList("app.rating_conf.company.country_origin.tier_1")
    tier_2 = load().getStringList("app.rating_conf.company.country_origin.tier_2")
    tier_points = load().getIntList("app.rating_conf.company.country_origin.tiers_points")

    val country_of_origin: DataFrame = df
      .withColumn("points",when(lower(col(category_column_name)).isin(tier_1: _*),tier_points(0))
        .when(lower(col(category_column_name)).isin(tier_2 : _*),tier_points(1))
        .when(!lower(col(category_column_name)).isin(tier_1 ++ tier_2 : _*) && col(category_column_name).isNotNull ,tier_points(2))
        .otherwise(tier_points(3)))

    val outputDataFrame: DataFrame = country_of_origin.select($"companynumber",country_of_origin.col("points"))

    outputDataFrame
  }

  def companiesCategory: DataFrame = {

    category_column_name = load().getString("app.rating_conf.company.category.main_column_name")
    tier_1 = load().getStringList("app.rating_conf.company.category.tier_1")
    tier_2 = load().getStringList("app.rating_conf.company.category.tier_2")
    tier_3 = load().getStringList("app.rating_conf.company.category.tier_3")
    tier_4 = load().getStringList("app.rating_conf.company.category.tier_4")
    tier_points = load().getIntList("app.rating_conf.company.category.tiers_points")

    val exact_df: DataFrame = df
      .select($"companynumber",df.col(category_column_name))
      .withColumn("points", when(lower(df.col(category_column_name)).isin(tier_1: _*),tier_points(0)))
      .where(lower(df.col(category_column_name)).isin(tier_1: _*))

    val percentage_df: DataFrame = df
      .groupBy(df.col(category_column_name))
      .agg(count(df.col(category_column_name)) as "count")
      .sort($"count".desc)
      .where(!lower(df.col(category_column_name)).isin(tier_1: _*))

    val retrive_max_count: Int = percentage_df
      .select(max($"count"))
      .first()
      .mkString("")
      .toInt

    val output_category: DataFrame = percentage_df
      .select(df.col(category_column_name), round($"count".cast("integer") * 100 / retrive_max_count) as ("percentage_points"))

    val percentage_df_output = df
      .select($"companynumber",df.col(category_column_name))
      .join(output_category,df.col(category_column_name) === output_category.col(category_column_name))
      .withColumn("points", when(lower($"percentage_points").between(tier_2(0),tier_2(1)),tier_points(1))
        .when(lower($"percentage_points").between(tier_3(0),tier_3(1)),tier_points(2))
        .when(lower($"percentage_points").between(tier_4(0),tier_4(1)),tier_points(3))
        .when(lower($"percentage_points").isNull,tier_points(4)))

    val outputDataFrame = percentage_df_output
      .select($"companynumber",$"points")
      .union(exact_df.select($"companynumber",$"points"))

    outputDataFrame
  }

  def companiesStatus: DataFrame = {

    category_column_name = load().getString("app.rating_conf.company.status.main_column_name")
    tier_1 = load().getStringList("app.rating_conf.company.status.tier_1")
    tier_2 = load().getStringList("app.rating_conf.company.status.tier_2")
    tier_points = load().getIntList("app.rating_conf.company.status.tiers_points")

    val status: DataFrame = df
      .withColumn("points",when(lower(df(category_column_name)).isin(tier_1: _*),tier_points(0))
        .when(lower(df(category_column_name)).isin(tier_2 : _*),tier_points(1))
        .when(!lower(df(category_column_name)).isin(tier_1 ++ tier_2 : _*) && df(category_column_name).isNotNull,tier_points(2))
        .when(df(category_column_name).isNull,tier_points(3)))

    val outputDataFrame: DataFrame = status.select($"companynumber",$"points")

    outputDataFrame
  }

  def companiesAge: DataFrame = {

    category_column_name = load().getString("app.rating_conf.company.age.main_column_name")
    tier_1 = load().getStringList("app.rating_conf.company.age.tier_1")
    tier_2 = load().getStringList("app.rating_conf.company.age.tier_2")
    tier_3 = load().getStringList("app.rating_conf.company.age.tier_3")
    tier_4 = load().getStringList("app.rating_conf.company.age.tier_4")
    tier_points = load().getIntList("app.rating_conf.company.age.tiers_points")

    val age: DataFrame = df
      .select($"companynumber",round(datediff(current_date(),to_date(df(category_column_name),"dd/MM/yyyy")) / 365).alias(category_column_name))

    val ageOutput = age.withColumn("points",when(age(category_column_name).between(tier_1(0),tier_1(1)),tier_points(0))
      .when(age(category_column_name).between(tier_2(0),tier_2(1)),tier_points(1))
      .when(age(category_column_name).between(tier_3(0),tier_3(1)),tier_points(2))
      .when(age(category_column_name).between(tier_4(0),tier_4(1)),tier_points(3))
      .otherwise(tier_points(4)))

    val outputDataFrame: DataFrame = ageOutput.select($"companynumber",$"points")

    outputDataFrame
  }

  def companiesAccCategory: DataFrame = {

    category_column_name = load().getString("app.rating_conf.company.accCategory.main_column_name")
    tier_1 = load().getStringList("app.rating_conf.company.accCategory.tier_1")
    tier_2 = load().getStringList("app.rating_conf.company.accCategory.tier_2")
    tier_3 = load().getStringList("app.rating_conf.company.accCategory.tier_3")
    tier_4 = load().getStringList("app.rating_conf.company.accCategory.tier_4")
    tier_5 = load().getStringList("app.rating_conf.company.accCategory.tier_5")
    tier_6 = load().getStringList("app.rating_conf.company.accCategory.tier_6")
    tier_points = load().getIntList("app.rating_conf.company.accCategory.tiers_points")

    val accCategory: DataFrame = df
      .select($"companynumber",df(category_column_name))
      .withColumn("points",when(lower(df(category_column_name)).isin(tier_1: _*),tier_points(0))
        .when(lower(df(category_column_name)).isin(tier_2 : _*),tier_points(1))
        .when(lower(df(category_column_name)).isin(tier_3 : _*),tier_points(2))
        .when(lower(df(category_column_name)).isin(tier_4 : _*),tier_points(3))
        .when(lower(df(category_column_name)).isin(tier_5 : _*),tier_points(4))
        .when(lower(df(category_column_name)).isin(tier_6 : _*) || df(category_column_name).isNull,tier_points(5)))

    val outputDataFrame: DataFrame = accCategory.select($"companynumber",$"points")

    outputDataFrame
  }

  def companiesSicCode: DataFrame ={
    category_column_name = load().getString("app.rating_conf.company.sicCode.main_column_name")

    val tier_1 = load().getList("app.rating_conf.company.sicCode.tier_1")
      .map(x => x.unwrapped().asInstanceOf[util.ArrayList[Int]]
      )

    val tier_2 = load().getList("app.rating_conf.company.sicCode.tier_2")
      .map(x => x.unwrapped().asInstanceOf[util.ArrayList[Int]]
      )

    tier_points = load().getIntList("app.rating_conf.company.sicCode.tiers_points")

    val age: DataFrame = df
      .select($"companynumber",substring(df(category_column_name),0,2).alias(category_column_name))

    val age_output = age.withColumn("points",
      when(age(category_column_name) === tier_1(0)(0)
        || age(category_column_name).between(tier_1(1)(0),tier_1(1)(1))
        || age(category_column_name).between(tier_1(2)(0),tier_1(2)(1))
        || age(category_column_name).between(tier_1(3)(0),tier_1(3)(1)), tier_points(0))
        .when(age(category_column_name) === tier_2(0)(0) || age(category_column_name).isNull,tier_points(2))
        .when(age(category_column_name).isNotNull ,tier_points(1)))

    val outputDataFrame: DataFrame = age_output.select($"companynumber",$"points")

    outputDataFrame
  }

  def companiesPostTown: DataFrame = {

    category_column_name = load().getString("app.rating_conf.company.postTown.main_column_name")
    tier_1 = load().getStringList("app.rating_conf.company.postTown.tier_1")
    tier_2 = load().getStringList("app.rating_conf.company.postTown.tier_2")
    tier_3 = load().getStringList("app.rating_conf.company.postTown.tier_3")
    tier_4 = load().getStringList("app.rating_conf.company.postTown.tier_4")
    tier_points = load().getIntList("app.rating_conf.company.postTown.tiers_points")

    val exact_df: DataFrame = df
      .select($"companynumber",df.col(category_column_name))
      .withColumn("points", when(lower(df.col(category_column_name)).isin(tier_1: _*),tier_points(0)))
      .where(lower(df.col(category_column_name)) === tier_1(0))

    val percentage_df: DataFrame = df
      .groupBy(df.col(category_column_name))
      .agg(count(df.col(category_column_name)) as "count")
      .sort($"count".desc)
      .where(!lower(df.col(category_column_name)).isin(tier_1: _*) && df.col(category_column_name) =!= "")

    val retrive_max_count: Int = percentage_df
      .select(max($"count"))
      .first()
      .mkString("")
      .toInt

    val output_category: DataFrame = percentage_df
      .select(df.col(category_column_name), round($"count".cast("integer") * 100 / retrive_max_count) as ("percentage_points"))

    val percentage_df_output = df
      .select($"companynumber",df.col(category_column_name))
      .join(output_category,df.col(category_column_name) === output_category.col(category_column_name))
      .withColumn("points", when($"percentage_points".between(tier_2(0),tier_2(1)),tier_points(1))
        .when($"percentage_points".between(tier_3(0),tier_3(1)),tier_points(2))
        .when($"percentage_points".between(tier_4(0),tier_4(1)),tier_points(3))
        .otherwise(tier_points(4)))

    val outputDataFrame = percentage_df_output
      .select($"companynumber",$"points")
      .union(exact_df.select($"companynumber",$"points"))

    outputDataFrame
  }

}

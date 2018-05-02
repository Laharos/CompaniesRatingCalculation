package running

import org.apache.spark.sql.{DataFrame, SaveMode}

class DataSave {

  def saveDataFrame(df: DataFrame, tableName: String): Unit = {
    df.write
       .format("com.databricks.spark.redshift")
       .option("url", "jdbc:redshift://")
       .option("tempdir", "s3a://")
       .option("dbtable", tableName)
       .option("aws_iam_role", "arn:aws:iam::")
       .mode(SaveMode.Overwrite)
       .save()
  }
}

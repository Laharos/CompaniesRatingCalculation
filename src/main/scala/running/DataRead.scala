package running
import java.util

import com.typesafe.config.ConfigFactory.load
import org.apache.spark.sql.DataFrame
import running.MainApp.spark

import scala.collection.mutable.ListBuffer

class DataRead {

  def getMainDF: DataFrame ={
	
   /* Local testing
	val aws_access_key_id: String = new EnvironmentVariableCredentialsProvider().getCredentials().getAWSAccessKeyId()
    val aws_secret_access_key: String =new EnvironmentVariableCredentialsProvider().getCredentials().getAWSSecretKey()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", aws_access_key_id)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint","s3-eu-central-1.amazonaws.com")*/
	
    val query = load().getString("app.unload.query")
    val df: DataFrame = spark.read.format("com.databricks.spark.redshift")
      .option("url","jdbc:redshift://")
      .option("query",query)
      .option("aws_iam_role","arn:aws:iam::")
      .option("tempdir","s3a://")
      .load()
    df
  }
  

}

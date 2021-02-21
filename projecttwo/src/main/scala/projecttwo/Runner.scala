package projecttwo
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future

object Runner {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("Project Two")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    //TrackTimeLocation.demoTrack(spark);
    //TrackDevice.tweetStream(spark)
    //TrackDevice.staticDataQuery(spark)
    //TrackGender.tweetStream(spark)
    TrackGender.staticDataQuery(spark)
  }
}
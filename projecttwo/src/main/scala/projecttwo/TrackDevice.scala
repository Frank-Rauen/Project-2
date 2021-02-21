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

object TrackDevice {

  def tweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(bearerToken, queryString = "?tweet.fields=source,geo&expansions=geo.place_id")
    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("twitterstreamDevice")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstreamDevice")

    staticDf.printSchema()

    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstreamDevice")

    streamDf
      .filter(!functions.isnull($"includes.places"))
      .groupBy("data.source")
      .count()
      .withColumnRenamed("count","Tot_Tweets_By_Device")
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

    val pattern = ".*(@\\w+)\\s+.*".r
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstreamDevice",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {

    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      s"https://api.twitter.com/2/tweets/sample/stream$queryString"
    )
    val httpGet = new HttpGet(uriBuilder.build())
    //set up the authorization for this request, using our bearer token
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      //initial filewriter, replaced every linesPerFile
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }
  def staticDataQuery(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("twitterstreamDevice/")
    
    //df.printSchema()
    df.filter(!functions.isnull($"includes.places"))
    .groupBy("data.source")
    .count()
    .withColumnRenamed("count","Tot_Tweets_By_Device")
    .withColumn("ratio", $"Tot_Tweets_By_Device"/functions.sum("Tot_Tweets_By_Device").over())
    .show()
  }

}
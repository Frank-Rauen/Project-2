package projecttwo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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

object LocationShareData {

  def locationTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    //grab a bearer token from the environment
    //never hardcode your tokens (never just put them as a string in your code)
    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")

    //writes all the tweets from twitter's stream into a directory
    // by default hits the sampled stream and uses "twitterstream" as the directory
    // We'll run it in the background using a Future:
    // We're not saving a reference to this future or providing a callback function
    // we just start it running in the background and forget about it.
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(bearerToken)
    }

    //Here we're just going to wait until a file appears in our twitterstream directory
    // or until some reasonable amount of time has passed (30s)
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("LocationShareDataTweetStream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    //We're going to start with a static DF
    // both to demo it, and to infer the schema
    // streaming dataframes can't infer schema
    val staticDf = spark.read.json("LocationShareDataTweetStream")

    //streamDf is a stream, using *Structured Streaming*
    val streamDf = spark.readStream.schema(staticDf.schema).json("LocationShareDataTweetStream")
    
    streamDf
    .select(($"includes.users.location").alias("Location"), ($"data.text").alias("Text"),($"includes.users.name").alias("Name"),($"data.author_id"))
    .groupBy("Location")
    .count().withColumnRenamed("count", "Total_Tweets_By_Location")
    .sort(desc("Total_Tweets_By_Location"))
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()
  }
  /**
    * An example running processing on groups and using DataSets instead of DataFrames.
    * 
    * To use DataSets, we need case classes to represent our data.
    *
    * @param spark
    */
  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "LocationShareDataTweetStream",
      linesPerFile: Int = 1000
  ) = {
    //a decent chunk of boilerplate -- from twitter docs/tutorial
    //sets up the request we're going to be sending to Twitter
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      "https://api.twitter.com/2/tweets/sample/stream?user.fields=location,name,created_at&expansions=author_id"
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
      var fileWriter = new PrintWriter(Paths.get("tweetstream.json").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.json"),
            Paths.get(s"$dirname/LocationShareDataTweetStream-$millis-${lineNumber/linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("tweetstream.json").toFile)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }
}
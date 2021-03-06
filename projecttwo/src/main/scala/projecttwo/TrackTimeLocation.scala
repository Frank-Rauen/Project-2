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

object TrackTimeLocation {

    def demoTrack(spark: SparkSession)
    {
        //helloTweetStream(spark)
        staticTweet(spark);
    }

    def staticTweet(spark: SparkSession): Unit = {
    import spark.implicits._

    //grab a bearer token from the environment
    //never hardcode your tokens (never just put them as a string in your code)
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    //Here we're just going to wait until a file appears in our twitterstream directory
    // or until some reasonable amount of time has passed (30s)
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("twitterstream1")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    //We're going to start with a static DF
    // both to demo it, and to infer the schema
    // streaming dataframes can't infer schema
    val staticDf = spark.read.json("twitterstream1")

    staticDf.printSchema()

    //streamDf is a stream, using *Structured Streaming*
    val staticDf1 = spark.read.option("inferSchema", "true").json("twitterstream1")
    val staticDf2 = spark.read.option("inferSchema", "true").json("twitterstream1")
    val df1 = staticDf
    .filter(functions.isnull($"includes.places"))
      .select(functions.hour($"data.created_at").as("Time_In_EST"))
      .groupBy("Time_In_EST")
      .count().withColumnRenamed("count","Tweets_Not_Having_Address")
    .toDF()

    val df2 = staticDf
    .filter(!functions.isnull($"includes.places"))
      .select(functions.hour($"data.created_at").as("Time_In_EST"))
      .groupBy("Time_In_EST")
      .count().withColumnRenamed("count","Tweets_Having_Address")
    .toDF()

    df1.join(df2,"Time_In_EST").sort(functions.desc("Tweets_Having_Address")).show()
  }
    

    def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    //grab a bearer token from the environment
    //never hardcode your tokens (never just put them as a string in your code)
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    

    //Here we're just going to wait until a file appears in our twitterstream directory
    // or until some reasonable amount of time has passed (30s)
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("twitterstream1")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    //writes all the tweets from twitter's stream into a directory
    // by default hits the sampled stream and uses "twitterstream" as the directory
    // We'll run it in the background using a Future:
    // We're not saving a reference to this future or providing a callback function
    // we just start it running in the background and forget about it.
    
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo,created_at&expansions=geo.place_id")
    }

    //We're going to start with a static DF
    // both to demo it, and to infer the schema
    // streaming dataframes can't infer schema
    val staticDf = spark.read.json("twitterstream1")

    staticDf.printSchema()
    val streamDf1 = spark.readStream.schema(staticDf.schema).json("twitterstream1")
    val streamDf2 = spark.readStream.schema(staticDf.schema).json("twitterstream1")

    streamDf1
        .filter(functions.isnull($"includes.places"))
      .select(functions.hour($"data.created_at").as("Time_In_EST"))
      .groupBy("Time_In_EST")
      .count().withColumnRenamed("count","Tweets_Not_Having_Address")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()

    streamDf2
        .filter(!functions.isnull($"includes.places"))
      .select(functions.hour($"data.created_at").as("Time_In_EST"))
      .groupBy("Time_In_EST")
      .count().withColumnRenamed("count","tweet_count_having_address")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  
}
    def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream1",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {
    //a decent chunk of boilerplate -- from twitter docs/tutorial
    //sets up the request we're going to be sending to Twitter
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
}
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

object LocationTweetUserObjCompare {

    def helloTweetStream(spark: SparkSession): Unit = {
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
      filesFoundInDir = Files.list(Paths.get("LocationTweetUserObjCompareTweetStream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    //We're going to start with a static DF
    // both to demo it, and to infer the schema
    // streaming dataframes can't infer schema
    
    val staticDf = spark.read.json("LocationTweetUserObjCompareTweetStream")

    //streamDf is a stream, using *Structured Streaming*
    val streamDf = spark.readStream.schema(staticDf.schema).json("LocationTweetUserObjCompareTweetStream")

    streamDf
    .filter(!functions.isnull($"includes.places"))
    .select(($"data.author_id"), ($"data.created_at").alias("Created"),($"includes.places").alias("Place"))
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
    //streamDf is a stream, using *Structured Streaming*
    
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
      dirname: String = "LocationTweetUserObjCompareTweetStream",
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
      "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=geo,created_at,author_id&expansions=geo.place_id"
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
      var fileWriter = new PrintWriter(Paths.get("comparetweetstream.json").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("comparetweetstream.json"),
            Paths.get(s"$dirname/LocationTweetUserObjCompareTweetStream-$millis-${lineNumber/linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("comparetweetstream.json").toFile)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }

  def staticDfGenerator(spark: SparkSession) {
    import spark.implicits._

    val staticDf = spark.read.option("header","true").json("LocationTweetUserObjCompareTweetStream").toDF()

    val staticDf2 = spark.read.option("header","true").json("LocationShareDataTweetStream").toDF()

    staticDf.join(staticDf2, "data.author_id")
    .filter(!functions.isnull($"includes.places"))
    .select(($"includes.users.location").alias("Location"), ($"includes.users.name").alias("Name"),($"includes.places").alias("Place"))
    .show()

    

  }
}
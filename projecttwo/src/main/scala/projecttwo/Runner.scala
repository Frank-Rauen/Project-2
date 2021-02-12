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

    //initialize a SparkSession, by convention called spark
    //SparkSession is the entrypoint for a Spark application using Spark SQL
    // it's new in Spark 2 + unifies older context objects.
    //SparkSession is different from SparkContext in that we can have multiple sessions
    // in the same runtime, where we only wanted 1 SparkContext per application.
    val spark = SparkSession
      .builder()
      .appName("Project Two")
      .master("local[4]")
      .getOrCreate()

    //we want to always add an import here, it enables some syntax and code generation:
    // if you run into mysterious errors with what should be working code, check to make sure this import exists
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    helloTweetStream(spark)

  }

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    //grab a bearer token from the environment
    //never hardcode your tokens (never just put them as a string in your code)
    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    //writes all the tweets from twitter's stream into a directory
    // by default hits the sampled stream and uses "twitterstream" as the directory
    // We'll run it in the background using a Future:
    // We're not saving a reference to this future or providing a callback function
    // we just start it running in the background and forget about it.
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo&expansions=geo.place_id")
    }

    //Here we're just going to wait until a file appears in our twitterstream directory
    // or until some reasonable amount of time has passed (30s)
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    //We're going to start with a static DF
    // both to demo it, and to infer the schema
    // streaming dataframes can't infer schema
    val staticDf = spark.read.json("twitterstream")

    staticDf.printSchema()

    //streamDf is a stream, using *Structured Streaming*
    val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

    //Display placenames as tweets occur.  Have to deal with a good chunk of nested data
    // Writing a case class and using DataSets would be more initial investment, but
    // would make writing queries like this much easier!
    streamDf
      .filter(!functions.isnull($"includes.places"))
      .select(functions.element_at($"includes.places", 1)("full_name").as("Place"), ($"data.text").as("Tweet"))
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

    //Example just getting the text:
    // streamDf
    //   .select($"data.text")
    //   .writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .start()
    //   .awaitTermination()

    //Most used twitter handles, aggregated over time:
    
    // regex to extract twitter handles
    val pattern = ".*(@\\w+)\\s+.*".r

    // streamDf
    //   .select($"data.text")
    //   .as[String]
    //   .flatMap(text => {text match {
    //     case pattern(handle) => {Some(handle)}
    //     case notFound => None
    //   }})
    //   .groupBy("value")
    //   .count()
    //   .sort(functions.desc("count"))
    //   .writeStream
    //   .outputMode("complete")
    //   .format("console")
    //   .start()
    //   .awaitTermination()
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream",
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

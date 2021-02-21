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
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.HttpURLConnection;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

object TrackGender {

  def tweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(bearerToken, queryString = "?tweet.fields=&expansions=geo.place_id,author_id")
    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("twitterstreamGender")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if(!filesFoundInDir) {
      println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
      System.exit(1)
    }

    val staticDf = spark.read.json("gender")

    staticDf.printSchema()

    val streamDf = spark.readStream.schema(staticDf.schema).json("gender")

    streamDf
      .groupBy("gender")
      .count()
      .withColumnRenamed("count","Tot_Tweets_By_Gender")
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
      dirname: String = "twitterstreamGender",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {

    val spark = SparkSession
      .builder()
      .appName("Project Two")
      .master("local[4]")
      .getOrCreate()
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
            streamGender(spark, s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}")
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }

    }
  }

  def streamGender(spark: SparkSession, recentStream: String): Unit = {
    import spark.implicits._
    val df = spark.read.json(recentStream)
    var nameArray = df.filter(!functions.isnull($"includes.places")).select(functions.element_at($"includes.users",1)("username")).collect()
   
    var i = 0
    var genderQuery = ""
    val dirname = "gender"
    var fileWriter = new PrintWriter(Paths.get("gender.tmp").toFile)
    var lineNumber = 1
    val millis = System.currentTimeMillis()
    for(i <- 0 to nameArray.length){
        genderQuery=nameArray(i).toString().substring(1, nameArray(i).toString().length()-1) + "&"

        try {
                val apiKey = "key=602fdc3e09b94c49a36dacf2"; //Your API Key
                val apiUrl = new URL(s"https://genderapi.io/twitter/?q=$genderQuery" + apiKey).openConnection.asInstanceOf[HttpURLConnection];

                if (apiUrl.getResponseCode() != 200) {
                    throw new RuntimeException("An server error: " + apiUrl.getResponseCode());
                }

                val iStream = new InputStreamReader(apiUrl.getInputStream());
                val bReader = new BufferedReader(iStream);
                val gson = new Gson();
                val jsonOb = gson.fromJson(bReader, classOf[JsonObject]);
                if(i == nameArray.length-1){
                    fileWriter.close()
                    Files.move(
                        Paths.get("gender.tmp"),
                        Paths.get(s"$dirname/gender-$millis-${lineNumber/nameArray.length-1}"))
                    }
                fileWriter.println(jsonOb.toString())
                lineNumber += 1
                apiUrl.disconnect();
            } catch {
                    case e: IOException => println("error")
                }
    }
  }

  def staticDataQuery(spark: SparkSession): Unit = {
    import spark.implicits._
    val df2 = spark.read.json("gender/")
    df2.groupBy("gender")
    .count()
    .withColumnRenamed("count","Tot_Tweets_By_Gender")
    .withColumn("ratio", $"Tot_Tweets_By_Gender"/functions.sum("Tot_Tweets_By_Gender").over())
    .show()
  }

}
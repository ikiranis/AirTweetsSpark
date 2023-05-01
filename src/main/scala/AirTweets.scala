import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lit, lower, regexp_replace, split}

object AirTweets {
    def main(args: Array[String]): Unit = {
        val ss = SparkSession
            .builder()
            .appName("AirTweets")
            .config("spark.master", "local")
            .getOrCreate()

        val inputFile = "input/tweets.csv"
        val outputDir = "output"

        // Διάβασμα του αρχείου csv
        val df = ss.read.option("header", "true").csv(inputFile)
            // Αφαίρεση σημείων στίξης από τη στήλη text. Μετατροπή όλων των χαρακτήρων σε πεζά.
            .withColumn("text", regexp_replace(col("text"), "[^A-Za-z0-9]+", " "))
            .withColumn("text", lower(col("text")))

        val sentiment_values = Seq("positive", "negative", "neutral")

        sentiment_values.foreach(sentiment => {
            println(s"Top 5 words for $sentiment tweets")

            df.select(explode(split(df("text"), "\\s+")).alias("word"))
                .filter(col("word") =!= "")
                .filter(col("airline_sentiment") === sentiment)
                .groupBy("word").count()
                .sort(col("count").desc)
                .show(5)
        })


    }

}

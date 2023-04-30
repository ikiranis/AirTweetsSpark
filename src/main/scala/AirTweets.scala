import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object AirTweets {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("AirTweet")
        val sc = new SparkContext(sparkConf) // create spark context

        val inputFile = "input/tweets.txt"
        val outputDir = "output"

        // Διάβασμα του αρχείου και αφαίρεση σημείων στίξης. Μετατροπή όλων των χαρακτήρων σε πεζά.
        val txtFile = sc.textFile(inputFile)
            .map(_.replaceAll("[^A-Za-z0-9]+", " "))
            .map(_.toLowerCase)
    }

}

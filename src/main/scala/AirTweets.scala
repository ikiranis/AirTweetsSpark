import org.apache.spark.sql.SparkSession

object AirTweets {
    def main(args: Array[String]): Unit = {
        val ss = SparkSession
            .builder()
            .appName("AirTweets")
            .config("spark.master", "local")
            .getOrCreate()

        val inputFile = "input/tweets.csv"
        val outputDir = "output"

        val df = ss.read.option("header", "true").csv(inputFile)

        // Show the first few lines of the DataFrame
        df.show()

        // Print the schema of the DataFrame
        df.printSchema()

        // Διάβασμα του αρχείου και αφαίρεση σημείων στίξης. Μετατροπή όλων των χαρακτήρων σε πεζά.
//        val txtFile = ss.textFile(inputFile)
//            .map(_.replaceAll("[^A-Za-z0-9]+", " "))
//            .map(_.toLowerCase)
    }

}

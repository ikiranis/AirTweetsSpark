import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, first, lower, regexp_replace, split}

object AirTweets {
    def main(args: Array[String]): Unit = {
        val ss = SparkSession
            .builder()
            .appName("AirTweets")
            .config("spark.master", "local")
            .getOrCreate()

        val inputFile = "input/tweets.csv"

        // Διάβασμα του αρχείου csv
        val df = ss.read.option("header", "true").csv(inputFile)
            // Αφαίρεση σημείων στίξης από τη στήλη text. Μετατροπή όλων των χαρακτήρων σε πεζά.
            .withColumn("text", regexp_replace(col("text"), "[^A-Za-z0-9]+", " "))
            .withColumn("text", lower(col("text")))

        // Δημιουργία μιας βοηθητικής λίστας με τις πιθανές τιμές της στήλης airline_sentiment, για να
        // γίνει διαπέραση με βάση αυτές τις τιμές
        val sentiment_values = Seq("positive", "negative", "neutral")

        // Για κάθε τιμή της λίστας sentiment_values, εκτύπωση των 5 πιο συχνών λέξεων. Θα τρέξει δηλαδή 3 φορές
        sentiment_values.foreach(sentiment => {
            println(s"Top 5 words for $sentiment tweets")

            df.select(explode(split(df("text"), " ")).alias("word")) // Σπάσιμο της στήλης text σε λέξεις
                .filter(col("word") =!= "") // Αφαίρεση των κενών λέξεων
                .filter(col("airline_sentiment") === sentiment) // Φιλτράρισμα μόνο των tweets με το συγκεκριμένο airline_sentiment
                .groupBy("word").count() // Ομαδοποίηση των λέξεων και μέτρηση του πλήθους τους
                .sort(col("count").desc) // Ταξινόμηση με βάση το πλήθος των λέξεων
                .show(5) // Εκτύπωση των 5 πρώτων λέξεων
        })

        // Εκτύπωση της κύριας αιτίας παραπόνων για κάθε αεροπορική εταιρεία
        df.filter(col("negativereason_confidence") > 0.5) // Φιλτράρισμα των tweets με negativereason_confidence > 0.5
            .groupBy("airline", "negativereason") // Ομαδοποίηση των tweets με βάση την αεροπορική εταιρεία και την αιτία παραπόνων
            .count() // Μέτρηση του πλήθους των tweets
            .sort(col("count").desc)  // Ταξινόμηση με βάση το πλήθος των tweets
            .groupBy("airline")  // Ομαδοποίηση των tweets με βάση την αεροπορική εταιρεία
            // Επιλογή της πρώτης αιτίας παραπόνων για κάθε αεροπορική εταιρεία και του πλήθους των tweets για αυτήν
            .agg(first("negativereason").alias("most_negative_reason"), first("count").alias("count_of_reason"))
            // Εκτύπωση των αποτελεσμάτων
            .foreach(row => {
                println(s"Most negative reason for ${row(0)} is ${row(1)}, with ${row(2)} tweets")
            })
    }

}

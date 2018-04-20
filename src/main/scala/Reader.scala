import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Logger, Level}

object Reader {
	def main(args: Array[String]) {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val sparkConf = new SparkConf().setAppName("Reader")
		val ssc = new StreamingContext(sparkConf, Seconds(10))

		val lines = ssc.textFileStream("Data/Live/")

		val words = lines.flatMap(_.split(" "))
		val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
		wordCounts.print()

		ssc.start()
		ssc.awaitTermination()
	}
}

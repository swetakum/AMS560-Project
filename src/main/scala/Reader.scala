import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming._
// {Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Reader {
	def main(args: Array[String]) {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val sparkConf = new SparkConf()
			.setAppName("Reader")
			.set("spark.driver.allowMultipleContexts", "true")
		val ssc = new StreamingContext(sparkConf, Seconds(3))

		val spark = SparkSession.builder.getOrCreate()
		val sc = new SparkContext(sparkConf)
		import spark.implicits._

		val lines = ssc.textFileStream("Data/Live/")

		val csvSchema = new StructType()
			.add("col1", IntegerType)
			.add("col2", IntegerType)
			.add("col3", IntegerType)

		def row(line: List[String]): Row = Row(line(0).toInt, line(1).toInt, line(2).toInt)

		var state = spark.createDataFrame(sc.emptyRDD[Row], csvSchema)


		lines.foreachRDD{ rdd =>
			val data = rdd.map(_.split(" ").to[List]).map(row)
			var df = spark.createDataFrame(data, csvSchema)
			// df = df.union(df)
			// df.count()
			state = state.union(df)
			// state.show()

			// df.createOrReplaceTempView("people")
	    // val sqlDf = spark.sql("SELECT AVG(col2) FROM people WHERE col3 > (SELECT AVG(col3) FROM people)")
	    // sqlDf.show()

			// state.printSchema()

			var ns = state
			ns.show()
		}

		// val words = lines.flatMap(_.split(" "))
		// val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
		// // wordCounts.print()
		// lines.print()


		ssc.start()
		ssc.awaitTermination()
	}
}

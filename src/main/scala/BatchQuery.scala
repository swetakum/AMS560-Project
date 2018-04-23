import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession

object BatchQuery {
	def main(args: Array[String]) {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder.appName("Batch").master("local").getOrCreate
    val inputPath = "Data/Batch"
    val csvSchema = new StructType().add("col1", StringType).add("col2", StringType)

    val df = spark.read.schema(csvSchema).csv(inputPath)
    df.show()

    df.createOrReplaceTempView("people")
    val sqlDf = spark.sql("SELECT * FROM people where col1 = 'a'")
    sqlDf.show()

  }
}

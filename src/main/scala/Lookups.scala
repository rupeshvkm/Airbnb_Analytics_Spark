import org.apache.spark.sql.functions._

trait Lookups extends DataIngestion {
  val full_moon_data = spark.
                        read.
                        option("header","true").csv("src/data/seed_full_moon_dates.csv").
                        select(to_date(col("full_moon_date")).as("full_moon_date"))

}

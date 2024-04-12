
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

trait CleansedModels extends DataIngestion {

  val spec = Window partitionBy(col("HOST_ID")) orderBy(col("CREATED_AT")) rowsBetween(-2,0)

  val listings_with_avg_min_nights: DataFrame = raw_listings.
    withColumn("ROLLING_MIN_NIGHTS_3_PRECEDING",round(avg(col("MINIMUM_NIGHTS")).over(spec),2))
  val listings_cleansed = raw_listings.selectExpr("ID as LISTING_ID","LISTING_URL","NAME as LISTING_NAME","ROOM_TYPE",
  "MINIMUM_NIGHTS","HOST_ID","CAST(REPLACE(PRICE,'$','') as double) as PRICE").persist()

  val hosts_cleansed = raw_hosts.select(col("ID").as("HOST_ID"),col("NAME").as("HOST_NAME"),col("IS_SUPERHOST"))
  val reviews_cleansed = raw_reviews.select(col("LISTING_ID"),to_date(col("DATE")).as("REVIEW_DATE"),col("REVIEWER_NAME"),
    col("COMMENTS").as("REVIEW_COMMENTS"),col("SENTIMENT").as("REVIEW_SENTIMENT"))

}

import org.apache.spark.sql.functions._

object AnalyticsModels extends App with DataIngestion with TransformedModels with Lookups {
  val reviews_full_moon_stays = reviews_cleansed.
    join(full_moon_data,col("full_moon_date") === date_sub(col("REVIEW_DATE"),1),"Left").
    withColumn("Is It Full Moon Stay?",when(col("full_moon_date").isNotNull,"Yes").otherwise("No"))

//  Analysis on customer's experience on staying airbnb properties on Full Moon days
  val full_moon_experience = reviews_full_moon_stays
    .groupBy("REVIEW_SENTIMENT","Is It Full Moon Stay?")
    .agg(count("REVIEW_COMMENTS"))

//  full_moon_experience.explain(mode = "extended")

//  overall user experience in all the listings

  val listing_with_reviews = reviews_full_moon_stays.join(listings_cleansed,Seq("LISTING_ID"),"left")

  listing_with_reviews.
    groupBy("LISTING_ID","LISTING_NAME","REVIEW_SENTIMENT").
    agg(count("REVIEW_COMMENTS").as("SENTIMENT #")).orderBy(desc("SENTIMENT #")).explain(mode="extended")

  spark.streams.awaitAnyTermination()
}

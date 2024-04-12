import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataIngestion {

  protected val spark = SparkSession.builder().master("local[*]").appName("SnowFlake-Data Ingestion").getOrCreate()
  SnowflakeConnectorUtils.disablePushdownSession(spark)
//  print(spark.conf.getAll)

  private var sfOptions = Map(
    "sfURL" -> "https://qw30062.us-east-2.aws.snowflakecomputing.com/",
    "sfAccount" -> "qw30062",
    "sfUser" -> "dbt",
    "sfPassword" -> "dbtPassword123",
    "sfDatabase" -> "AIRBNB",
    "sfSchema" -> "RAW",
    "sfRole" -> "transform",
    "TIMEZONE" -> "America/Los_Angeles"
  )

  val raw_listings: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable","RAW_LISTINGS")
    .load()
    .repartition(4)

  val raw_hosts: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable","RAW_HOSTS")
    .load()

  val raw_reviews: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake") // or just use "snowflake"
    .options(sfOptions)
    .option("dbtable","RAW_REVIEWS")
    .load()

  println("=======================================================>","raw_listings")
  raw_listings.printSchema()
  println("=======================================================>","raw_hosts")
  raw_hosts.printSchema()
  println("=======================================================>","raw_reviews")
  raw_reviews.printSchema()

}

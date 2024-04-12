== Parsed Logical Plan ==
'Aggregate [REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293], [REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293, count('REVIEW_COMMENTS) AS count(REVIEW_COMMENTS)#310]
+- Project [LISTING_ID#28, REVIEW_DATE#242, REVIEWER_NAME#30, REVIEW_COMMENTS#243, REVIEW_SENTIMENT#244, full_moon_date#278, CASE WHEN isnotnull(full_moon_date#278) THEN Yes ELSE No END AS Is It Full Moon Stay?#293]
+- Join LeftOuter, (full_moon_date#278 = date_sub(REVIEW_DATE#242, 1))
:- Project [LISTING_ID#28, to_date(DATE#29, None, Some(Asia/Calcutta)) AS REVIEW_DATE#242, REVIEWER_NAME#30, COMMENTS#31 AS REVIEW_COMMENTS#243, SENTIMENT#32 AS REVIEW_SENTIMENT#244]
:  +- Relation [LISTING_ID#28,DATE#29,REVIEWER_NAME#30,COMMENTS#31,SENTIMENT#32] SnowflakeRelation
+- Project [to_date(full_moon_date#276, None, Some(Asia/Calcutta)) AS full_moon_date#278]
+- Relation [full_moon_date#276] csv

== Analyzed Logical Plan ==
REVIEW_SENTIMENT: string, Is It Full Moon Stay?: string, count(REVIEW_COMMENTS): bigint
Aggregate [REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293], [REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293, count(REVIEW_COMMENTS#243) AS count(REVIEW_COMMENTS)#310L]
+- Project [LISTING_ID#28, REVIEW_DATE#242, REVIEWER_NAME#30, REVIEW_COMMENTS#243, REVIEW_SENTIMENT#244, full_moon_date#278, CASE WHEN isnotnull(full_moon_date#278) THEN Yes ELSE No END AS Is It Full Moon Stay?#293]
+- Join LeftOuter, (full_moon_date#278 = date_sub(REVIEW_DATE#242, 1))
:- Project [LISTING_ID#28, to_date(DATE#29, None, Some(Asia/Calcutta)) AS REVIEW_DATE#242, REVIEWER_NAME#30, COMMENTS#31 AS REVIEW_COMMENTS#243, SENTIMENT#32 AS REVIEW_SENTIMENT#244]
:  +- Relation [LISTING_ID#28,DATE#29,REVIEWER_NAME#30,COMMENTS#31,SENTIMENT#32] SnowflakeRelation
+- Project [to_date(full_moon_date#276, None, Some(Asia/Calcutta)) AS full_moon_date#278]
+- Relation [full_moon_date#276] csv

== Optimized Logical Plan ==
Aggregate [REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293], [REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293, count(REVIEW_COMMENTS#243) AS count(REVIEW_COMMENTS)#310L]
+- Project [REVIEW_COMMENTS#243, REVIEW_SENTIMENT#244, CASE WHEN isnotnull(full_moon_date#278) THEN Yes ELSE No END AS Is It Full Moon Stay?#293]
+- Join LeftOuter, (full_moon_date#278 = date_sub(REVIEW_DATE#242, 1))
:- Project [cast(DATE#29 as date) AS REVIEW_DATE#242, COMMENTS#31 AS REVIEW_COMMENTS#243, SENTIMENT#32 AS REVIEW_SENTIMENT#244]
:  +- Relation [LISTING_ID#28,DATE#29,REVIEWER_NAME#30,COMMENTS#31,SENTIMENT#32] SnowflakeRelation
+- Project [cast(full_moon_date#276 as date) AS full_moon_date#278]
+- Filter isnotnull(cast(full_moon_date#276 as date))
+- Relation [full_moon_date#276] csv

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293], functions=[count(REVIEW_COMMENTS#243)], output=[REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293, count(REVIEW_COMMENTS)#310L])
+- Exchange hashpartitioning(REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293, 200), ENSURE_REQUIREMENTS, [plan_id=94]
+- HashAggregate(keys=[REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293], functions=[partial_count(REVIEW_COMMENTS#243)], output=[REVIEW_SENTIMENT#244, Is It Full Moon Stay?#293, count#315L])
+- Project [REVIEW_COMMENTS#243, REVIEW_SENTIMENT#244, CASE WHEN isnotnull(full_moon_date#278) THEN Yes ELSE No END AS Is It Full Moon Stay?#293]
+- BroadcastHashJoin [date_sub(REVIEW_DATE#242, 1)], [full_moon_date#278], LeftOuter, BuildRight, false
:- SnowflakePlan [REVIEW_DATE#242, REVIEW_COMMENTS#243, REVIEW_SENTIMENT#244], SnowflakeResultSetRDD[32] at RDD at SnowflakeResultSetRDD.scala:41
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, date, true]),false), [plan_id=89]
+- Project [cast(full_moon_date#276 as date) AS full_moon_date#278]
+- Filter isnotnull(cast(full_moon_date#276 as date))
+- FileScan csv [full_moon_date#276] Batched: false, DataFilters: [isnotnull(cast(full_moon_date#276 as date))], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/C:/Users/mrupv/IdeaProjects/Airbnb Spark Analytics/src/data/seed..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<full_moon_date:string>

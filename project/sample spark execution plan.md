== Parsed Logical Plan ==
'Sort ['SENTIMENT # DESC NULLS LAST], true
+- Aggregate [LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103], [LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103, count(REVIEW_COMMENTS#102) AS SENTIMENT ##201L]
+- Project [LISTING_ID#28, REVIEW_DATE#101, REVIEWER_NAME#30, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103, full_moon_date#137, Is It Full Moon Stay?#152, LISTING_URL#1, LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, PRICE#53]
+- Join LeftOuter, (LISTING_ID#28 = LISTING_ID#51)
:- Project [LISTING_ID#28, REVIEW_DATE#101, REVIEWER_NAME#30, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103, full_moon_date#137, CASE WHEN isnotnull(full_moon_date#137) THEN Yes ELSE No END AS Is It Full Moon Stay?#152]
:  +- Join LeftOuter, (full_moon_date#137 = date_sub(REVIEW_DATE#101, 1))
:     :- Project [LISTING_ID#28, to_date(DATE#29, None, Some(Asia/Calcutta)) AS REVIEW_DATE#101, REVIEWER_NAME#30, COMMENTS#31 AS REVIEW_COMMENTS#102, SENTIMENT#32 AS REVIEW_SENTIMENT#103]
:     :  +- Relation [LISTING_ID#28,DATE#29,REVIEWER_NAME#30,COMMENTS#31,SENTIMENT#32] SnowflakeRelation
:     +- Project [to_date(full_moon_date#135, None, Some(Asia/Calcutta)) AS full_moon_date#137]
:        +- Relation [full_moon_date#135] csv
+- Project [ID#0 AS LISTING_ID#51, LISTING_URL#1, NAME#2 AS LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, cast(replace(PRICE#6, $, ) as double) AS PRICE#53]
+- Repartition 4, true
+- Relation [ID#0,LISTING_URL#1,NAME#2,ROOM_TYPE#3,MINIMUM_NIGHTS#4,HOST_ID#5,PRICE#6,CREATED_AT#7,UPDATED_AT#8] SnowflakeRelation

== Analyzed Logical Plan ==
LISTING_ID: decimal(38,0), LISTING_NAME: string, REVIEW_SENTIMENT: string, SENTIMENT #: bigint
Sort [SENTIMENT ##201L DESC NULLS LAST], true
+- Aggregate [LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103], [LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103, count(REVIEW_COMMENTS#102) AS SENTIMENT ##201L]
+- Project [LISTING_ID#28, REVIEW_DATE#101, REVIEWER_NAME#30, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103, full_moon_date#137, Is It Full Moon Stay?#152, LISTING_URL#1, LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, PRICE#53]
+- Join LeftOuter, (LISTING_ID#28 = LISTING_ID#51)
:- Project [LISTING_ID#28, REVIEW_DATE#101, REVIEWER_NAME#30, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103, full_moon_date#137, CASE WHEN isnotnull(full_moon_date#137) THEN Yes ELSE No END AS Is It Full Moon Stay?#152]
:  +- Join LeftOuter, (full_moon_date#137 = date_sub(REVIEW_DATE#101, 1))
:     :- Project [LISTING_ID#28, to_date(DATE#29, None, Some(Asia/Calcutta)) AS REVIEW_DATE#101, REVIEWER_NAME#30, COMMENTS#31 AS REVIEW_COMMENTS#102, SENTIMENT#32 AS REVIEW_SENTIMENT#103]
:     :  +- Relation [LISTING_ID#28,DATE#29,REVIEWER_NAME#30,COMMENTS#31,SENTIMENT#32] SnowflakeRelation
:     +- Project [to_date(full_moon_date#135, None, Some(Asia/Calcutta)) AS full_moon_date#137]
:        +- Relation [full_moon_date#135] csv
+- Project [ID#0 AS LISTING_ID#51, LISTING_URL#1, NAME#2 AS LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, cast(replace(PRICE#6, $, ) as double) AS PRICE#53]
+- Repartition 4, true
+- Relation [ID#0,LISTING_URL#1,NAME#2,ROOM_TYPE#3,MINIMUM_NIGHTS#4,HOST_ID#5,PRICE#6,CREATED_AT#7,UPDATED_AT#8] SnowflakeRelation

== Optimized Logical Plan ==
Sort [SENTIMENT ##201L DESC NULLS LAST], true
+- Aggregate [LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103], [LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103, count(REVIEW_COMMENTS#102) AS SENTIMENT ##201L]
+- Project [LISTING_ID#28, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103, LISTING_NAME#52]
+- Join LeftOuter, (LISTING_ID#28 = LISTING_ID#51)
:- Project [LISTING_ID#28, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103]
:  +- Join LeftOuter, (full_moon_date#137 = date_sub(REVIEW_DATE#101, 1))
:     :- Project [LISTING_ID#28, cast(DATE#29 as date) AS REVIEW_DATE#101, COMMENTS#31 AS REVIEW_COMMENTS#102, SENTIMENT#32 AS REVIEW_SENTIMENT#103]
:     :  +- Relation [LISTING_ID#28,DATE#29,REVIEWER_NAME#30,COMMENTS#31,SENTIMENT#32] SnowflakeRelation
:     +- Project [cast(full_moon_date#135 as date) AS full_moon_date#137]
:        +- Filter isnotnull(cast(full_moon_date#135 as date))
:           +- Relation [full_moon_date#135] csv
+- Project [LISTING_ID#51, LISTING_NAME#52]
+- Filter isnotnull(LISTING_ID#51)
+- InMemoryRelation [LISTING_ID#51, LISTING_URL#1, LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, PRICE#53], StorageLevel(disk, memory, deserialized, 1 replicas)
+- *(1) Project [ID#0 AS LISTING_ID#51, LISTING_URL#1, NAME#2 AS LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, cast(replace(PRICE#6, $, ) as double) AS PRICE#53]
+- Exchange RoundRobinPartitioning(4), REPARTITION_BY_NUM, [plan_id=36]
+- SnowflakePlan [ID#0, LISTING_URL#1, NAME#2, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, PRICE#6], SnowflakeResultSetRDD[0] at RDD at SnowflakeResultSetRDD.scala:41

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [SENTIMENT ##201L DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(SENTIMENT ##201L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=99]
+- HashAggregate(keys=[LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103], functions=[count(REVIEW_COMMENTS#102)], output=[LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103, SENTIMENT ##201L])
+- HashAggregate(keys=[LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103], functions=[partial_count(REVIEW_COMMENTS#102)], output=[LISTING_ID#28, LISTING_NAME#52, REVIEW_SENTIMENT#103, count#312L])
+- Project [LISTING_ID#28, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103, LISTING_NAME#52]
+- SortMergeJoin [LISTING_ID#28], [LISTING_ID#51], LeftOuter
:- Sort [LISTING_ID#28 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(LISTING_ID#28, 200), ENSURE_REQUIREMENTS, [plan_id=90]
:     +- Project [LISTING_ID#28, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103]
:        +- BroadcastHashJoin [date_sub(REVIEW_DATE#101, 1)], [full_moon_date#137], LeftOuter, BuildRight, false
:           :- SnowflakePlan [LISTING_ID#28, REVIEW_DATE#101, REVIEW_COMMENTS#102, REVIEW_SENTIMENT#103], SnowflakeResultSetRDD[11] at RDD at SnowflakeResultSetRDD.scala:41
:           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, date, true]),false), [plan_id=85]
:              +- Project [cast(full_moon_date#135 as date) AS full_moon_date#137]
:                 +- Filter isnotnull(cast(full_moon_date#135 as date))
:                    +- FileScan csv [full_moon_date#135] Batched: false, DataFilters: [isnotnull(cast(full_moon_date#135 as date))], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/C:/Users/mrupv/IdeaProjects/Airbnb Spark Analytics/src/data/seed..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<full_moon_date:string>
+- Sort [LISTING_ID#51 ASC NULLS FIRST], false, 0
+- Exchange hashpartitioning(LISTING_ID#51, 200), ENSURE_REQUIREMENTS, [plan_id=91]
+- Filter isnotnull(LISTING_ID#51)
+- InMemoryTableScan [LISTING_ID#51, LISTING_NAME#52], [isnotnull(LISTING_ID#51)]
+- InMemoryRelation [LISTING_ID#51, LISTING_URL#1, LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, PRICE#53], StorageLevel(disk, memory, deserialized, 1 replicas)
+- *(1) Project [ID#0 AS LISTING_ID#51, LISTING_URL#1, NAME#2 AS LISTING_NAME#52, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, cast(replace(PRICE#6, $, ) as double) AS PRICE#53]
+- Exchange RoundRobinPartitioning(4), REPARTITION_BY_NUM, [plan_id=102]
+- SnowflakePlan [ID#0, LISTING_URL#1, NAME#2, ROOM_TYPE#3, MINIMUM_NIGHTS#4, HOST_ID#5, PRICE#6], SnowflakeResultSetRDD[0] at RDD at SnowflakeResultSetRDD.scala:41


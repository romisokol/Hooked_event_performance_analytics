# Databricks notebook source
file_path = "/Volumes/<catalog>/<schema>/<volume>/hooked_events_sample.csv"
# Replace this path with the location of your own uploaded CSV file.

print(file_path)

# COMMAND ----------

from pyspark.sql import functions as F

raw_file_df = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "false")
    .csv(file_path)
)

display(raw_file_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

numbered_df = raw_file_df.withColumn(
    "row_number",
    F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
)

events_bronze = (
    numbered_df
    .filter(F.col("row_number") >= 11)
    .select(
        F.col("_c1").alias("event"),
        F.col("_c2").alias("event_type"),
        F.col("_c3").alias("city"),
        F.col("_c4").alias("state"),
        F.col("_c5").alias("country"),
        F.col("_c6").alias("event_date_raw"),
        F.col("_c7").alias("estimated_attendees_raw"),
        F.col("_c8").alias("estimated_singles_raw"),
        F.col("_c9").alias("total_users_raw"),
        F.col("_c10").alias("matches_raw"),
        F.col("_c11").alias("messages_raw"),
        F.col("_c12").alias("likes_raw")
    )
    .filter(F.col("event").isNotNull())
)

display(events_bronze)

# COMMAND ----------

from pyspark.sql.types import IntegerType

events_silver = (
    events_bronze

    # Remove extra spaces
    .withColumn("event", F.trim(F.col("event")))
    .withColumn("event_type", F.trim(F.col("event_type")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("country", F.trim(F.col("country")))

    # Turn blank strings into null values
    .replace("", None)

    # Standardize city names
    .withColumn(
        "city",
        F.when(
            F.upper(F.col("city")).isin("TLV", "TEL AVIV"),
            "Tel Aviv"
        ).otherwise(F.col("city"))
    )

    # Standardize country names
    .withColumn(
        "country",
        F.when(
            F.upper(F.col("country")).isin("IL", "ISRAEL"),
            "Israel"
        )
        .when(
            F.upper(F.col("country")) == "USA",
            "United States"
        )
        .otherwise(F.col("country"))
    )

    # Correct obvious spelling errors
    .withColumn(
        "state",
        F.when(F.lower(F.col("state")) == "floirda", "Florida")
        .when(F.lower(F.col("state")) == "massachutes", "Massachusetts")
        .otherwise(F.col("state"))
    )

    # Convert the date text into a real date
    .withColumn(
        "event_date",
        F.try_to_date(F.col("event_date_raw"), "M/dd/yyyy")
    )

    # Convert number columns from text into integers
    .withColumn(
        "estimated_attendees",
        F.col("estimated_attendees_raw").cast(IntegerType())
    )
    .withColumn(
        "estimated_singles",
        F.col("estimated_singles_raw").cast(IntegerType())
    )
    .withColumn(
        "total_users",
        F.col("total_users_raw").cast(IntegerType())
    )
    .withColumn(
        "matches",
        F.col("matches_raw").cast(IntegerType())
    )
    .withColumn(
        "messages",
        F.col("messages_raw").cast(IntegerType())
    )
    .withColumn(
        "likes",
        F.col("likes_raw").cast(IntegerType())
    )

    # Mark rows as complete or incomplete
    .withColumn(
        "record_status",
        F.when(
            F.col("total_users").isNotNull()
            & F.col("matches").isNotNull()
            & F.col("messages").isNotNull()
            & F.col("likes").isNotNull(),
            "Complete"
        ).otherwise("Incomplete")
    )

    # Keep only the useful final columns
    .select(
        "event",
        "event_type",
        "city",
        "state",
        "country",
        "event_date",
        "estimated_attendees",
        "estimated_singles",
        "total_users",
        "matches",
        "messages",
        "likes",
        "record_status"
    )
)

display(events_silver)

# COMMAND ----------

display(
    events_silver
    .groupBy("record_status")
    .count()
)

# COMMAND ----------

hooked_event_performance = (
    events_silver
    .filter(F.col("record_status") == "Complete")

    # Adoption: percentage of estimated singles who became users
    .withColumn(
        "adoption_rate",
        F.when(
            F.col("estimated_singles") > 0,
            F.col("total_users") / F.col("estimated_singles")
        )
    )

    # Activity per user
    .withColumn(
        "matches_per_user",
        F.when(
            F.col("total_users") > 0,
            F.col("matches") / F.col("total_users")
        )
    )

    .withColumn(
        "messages_per_user",
        F.when(
            F.col("total_users") > 0,
            F.col("messages") / F.col("total_users")
        )
    )

    .withColumn(
        "likes_per_user",
        F.when(
            F.col("total_users") > 0,
            F.col("likes") / F.col("total_users")
        )
    )

    # Overall engagement
    .withColumn(
        "total_engagement",
        F.col("matches") + F.col("messages") + F.col("likes")
    )

    .withColumn(
        "engagement_per_user",
        F.when(
            F.col("total_users") > 0,
            (
                F.col("matches")
                + F.col("messages")
                + F.col("likes")
            ) / F.col("total_users")
        )
    )

    # Useful categories
    .withColumn(
        "event_month",
        F.date_trunc("month", F.col("event_date"))
    )

    .withColumn(
        "event_size",
        F.when(F.col("estimated_attendees") < 200, "Small")
        .when(F.col("estimated_attendees") < 500, "Medium")
        .otherwise("Large")
    )
)

display(hooked_event_performance)

# COMMAND ----------

display(
    hooked_event_performance
    .select(
        "event",
        "event_type",
        "country",
        "estimated_singles",
        "total_users",
        F.round(F.col("adoption_rate") * 100, 1).alias("adoption_rate_percent"),
        F.round(F.col("engagement_per_user"), 2).alias("engagement_per_user")
    )
    .orderBy(F.desc("adoption_rate"))
)

# COMMAND ----------

events_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.hooked_events_bronze")

events_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.hooked_events_silver")

hooked_event_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.hooked_event_performance")

print("All three tables were saved successfully.")

# COMMAND ----------

display(
    spark.table("workspace.default.hooked_event_performance")
    .limit(10)
)

# COMMAND ----------

from pyspark.sql.window import Window

top_adoption_events_chart = (
    hooked_event_performance
    .filter(F.col("estimated_singles") >= 30)
    .select(
        "event",
        F.round(
            F.col("adoption_rate") * 100,
            1
        ).alias("adoption_rate_percent")
    )
    .withColumn(
        "rank",
        F.row_number().over(
            Window.orderBy(F.desc("adoption_rate_percent"))
        )
    )
    .filter(F.col("rank") <= 10)
    .withColumn(
        "ranked_event",
        F.concat(
            F.lpad(F.col("rank").cast("string"), 2, "0"),
            F.lit(". "),
            F.col("event")
        )
    )
    .select(
        "ranked_event",
        "adoption_rate_percent"
    )
)

display(top_adoption_events_chart)

# COMMAND ----------

event_type_performance = (
    hooked_event_performance
    .filter(F.col("event_type").isNotNull())
    .groupBy("event_type")
    .agg(
        F.count("*").alias("number_of_events"),
        F.round(
            F.avg(F.col("adoption_rate") * 100),
            1
        ).alias("average_adoption_rate_percent")
    )
    .orderBy(F.desc("average_adoption_rate_percent"))
)

display(event_type_performance)

# COMMAND ----------

event_type_engagement = (
    hooked_event_performance
    .filter(F.col("event_type").isNotNull())
    .groupBy("event_type")
    .agg(
        F.count("*").alias("number_of_events"),
        F.round(
            F.avg("engagement_per_user"),
            2
        ).alias("average_engagement_per_user")
    )
    .orderBy(F.desc("average_engagement_per_user"))
)

display(event_type_engagement)

# COMMAND ----------

monthly_performance = (
    hooked_event_performance
    .filter(F.col("event_month").isNotNull())
    .groupBy("event_month")
    .agg(
        F.count("*").alias("number_of_events"),
        F.sum("total_users").alias("total_users"),
        F.sum("total_engagement").alias("total_engagement")
    )
    .orderBy("event_month")
)

display(monthly_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings
# MAGIC
# MAGIC - Sporting events had the highest average adoption rate.
# MAGIC - Festivals generated the highest average engagement per user.
# MAGIC - Pitch a Friend had the strongest individual event adoption rate.
# MAGIC - High adoption and high engagement came from different event types.
# MAGIC - Monthly performance varied significantly, with a major spike in May 2026.
# MAGIC
# MAGIC ## Business Recommendations
# MAGIC
# MAGIC - Prioritize event formats that perform well on both adoption and engagement.
# MAGIC - Evaluate event success using both total activity and per-user metrics.
# MAGIC - Investigate the events behind the May 2026 spike to understand what drove performance.
# MAGIC - Continue improving data completeness for future events.
# MAGIC - Use estimated-singles assumptions carefully because adoption can exceed 100%.

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col,expr
from pyspark.sql.functions import desc,unix_timestamp, avg, count, dayofweek

spark = SparkSession.builder \
    .appName("Capstone") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.table("capstone.accident")
# Find the most frequent weather condition
most_frequent_weather = df.groupBy('weather_condition').count().orderBy(desc('count')).first()['weather_condition']

# Replace missing values with the most frequent weather condition
df = df.fillna({'weather_condition': most_frequent_weather})

# Create a new column 'incident_year' to extract the year from 'start_time'
df = df.withColumn("incident_year", expr("year(start_time)"))

# Join with stateMappingData
stateMappingData = spark.createDataFrame([
          ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DC", "District of Columbia"),
    ("DE", "Delaware"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("HI", "Hawaii"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("IA", "Iowa"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("ME", "Maine"),
    ("MD", "Maryland"),
    ("MA", "Massachusetts"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MS", "Mississippi"),
    ("MO", "Missouri"),
    ("MT", "Montana"),
    ("NE", "Nebraska"),
    ("NV", "Nevada"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NY", "New York"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VT", "Vermont"),
    ("VA", "Virginia"),
    ("WA", "Washington"),
    ("WV", "West Virginia"),
    ("WI", "Wisconsin"),
    ("WY", "Wyoming")], ["state", "state_name"])

# Perform a left join to map state names while preserving the original data
df = df.join(stateMappingData, on="state", how="left")
df = df.filter(df.id != "A-6529351")
# Replace null values in "state" with the original "state" column value
df = df.withColumn("state_name", when(col("state_name").isNull(), col("state")).otherwise(col("state_name")))

# Drop the "state_name" column if it's no longer needed
df = df.drop("state").withColumnRenamed("state_name", "state")
# Create a new column 'severity_category'

df = df.withColumn("severity_category",
                                   when(col("severity") == 1, "Low")
                                   .when(col("severity") == 2, "Medium")
                                   .when(col("severity") == 3, "High")
                                   .otherwise("High"))

# Calculate the duration in minutes"""
df = df.withColumn("duration_minutes",
                                       (unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) / 60)

# Calculate the duration in hours
df = df.withColumn("duration_hours",
                                           (unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) / 3600)

# Filter high severity incidents with duration > 60 minutes
filteredIncidents = df.filter((col("severity") == 3) & (col("duration_minutes") > 60))

# Group by severity_category and pivot on weather_condition, then calculate the average duration
summaryPivot = df.groupBy("severity_category") \
    .pivot("weather_condition") \
    .agg(avg("duration_minutes"))

# Extract the day of the week from the 'start_time' column
df = df.withColumn("day_of_week", dayofweek(col("start_time")))

# Group incidents by day of the week and calculate the incident count
dayOfWeekAggregated = df.groupBy("day_of_week") \
    .agg(count("*").alias("incident_count")) \
    .orderBy("day_of_week")

# Analyze accidents by city and sort by incident count
cityAggregated = df.groupBy("city") \
    .agg(count("*").alias("incident_count")) \
    .orderBy(col("incident_count").desc())

# Analyze accidents by state and severity
stateSeverityAggregated = df.groupBy("state", "severity_category") \
    .agg(count("*").alias("incident_count")) \
    .orderBy("state", "severity_category")

# Rename columns
df = df.withColumnRenamed("start_lat", "latitude_start")
df = df.withColumnRenamed("start_lng", "longitude_start")
df = df.withColumnRenamed("end_lat", "latitude_end")
df = df.withColumnRenamed("end_lng", "longitude_end")
df.show()
# Show the results
filteredIncidents.show()
#summaryPivot.show()
dayOfWeekAggregated.show()
cityAggregated.show()
stateSeverityAggregated.show()


# create table from df
df.createOrReplaceTempView("capstone_table")

# Top 5 cities with the highest number of accidents
top_cities_accidents = spark.sql("""
    SELECT city, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY city
    ORDER BY accident_count DESC
    LIMIT 5
""")
top_cities_accidents.show()

# Top 3 states having road accidents
top_states_accidents = spark.sql("""
    SELECT state, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY state
    ORDER BY accident_count DESC
    LIMIT 3
""")
top_states_accidents.show()

# Year-wise road accidents trend
accidents_by_year = spark.sql("""
    SELECT incident_year, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY incident_year
    ORDER BY incident_year
""")
accidents_by_year.show()

# Accidents by Weather Condition
accidents_by_weather = spark.sql("""
    SELECT weather_condition, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY weather_condition
    ORDER BY accident_count DESC
""")
accidents_by_weather.show()

# Accidents by Day of the Week
accidents_by_day_of_week = spark.sql("""
    SELECT day_of_week, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY day_of_week
    ORDER BY accident_count DESC
""")
accidents_by_day_of_week.show()

# Accidents by Month
accidents_by_month = spark.sql("""
    SELECT month(start_time) AS accident_month, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY accident_month
    ORDER BY accident_month
""")
accidents_by_month.show()

df.write.mode("overwrite").option("header", "true").saveAsTable("capstone.accident_transform")

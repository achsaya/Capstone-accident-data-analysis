from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col,expr
from pyspark.sql.functions import desc,unix_timestamp, avg, count, dayofweek,hour

spark = SparkSession.builder \
    .appName("Capstone") \
    .enableHiveSupport() \
    .getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
df = spark.read.table("capstone.accident_new")
# Find the most frequent weather condition
most_frequent_weather = df.groupBy('weather_condition').count().orderBy(desc('count')).first()['weather_condition']

# Replace missing values with the most frequent weather condition
df = df.fillna({'weather_condition': most_frequent_weather})

# Create a new column 'year' and 'month' to extract the year and month from 'start_time'
df = df.withColumn("year", expr("year(start_time)"))
df = df.withColumn("month", expr("month(start_time)"))

month_mapping = spark.createDataFrame([
    (1, "January"),
    (2, "February"),
    (3, "March"),
    (4, "April"),
    (5, "May"),
    (6, "June"),
    (7, "July"),
    (8, "August"),
    (9, "September"),
    (10, "October"),
    (11, "November"),
    (12, "December")],["month","monthname"])

# Convert numeric month to month name using a lookup table
df = df.join(month_mapping, on="month", how="left")
df = df.drop("month").withColumnRenamed("monthname", "month")
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

# Extract the day of the week from the 'start_time' column
df = df.withColumn("day_of_week", dayofweek(col("start_time")))
# Define a mapping from day-of-week number to day name
day_mapping = spark.createDataFrame([
    (1, "Sunday"),
    (2, "Monday"),
    (3, "Tuesday"),
    (4, "Wednesday"),
    (5, "Thursday"),
    (6, "Friday"),
    (7, "Saturday")
], ["day_of_week", "day_name"])

# Join the mapping DataFrame to replace day-of-week values with day names
df = df.join(day_mapping, on="day_of_week", how="left")
df = df.drop("day_of_week").withColumnRenamed("day_name", "day_of_week")
# Define the condition to determine day or night
day_night_expr = when((hour(df["Start_Time"]) >= 6) & (hour(df["Start_Time"]) < 18), "day").otherwise("night")

# Add the 'day_night' column to the DataFrame
df = df.withColumn("day_night", day_night_expr)

#-----------------------------------------------------------------------------------------------------------------------------------------------
#Analysis Part

# Group incidents by day of the week and calculate the incident count
dayOfWeekAggregated = df.groupBy("day_of_week") \
    .agg(count("*").alias("incident_count")) \
    .orderBy("day_of_week")
accident_count_by_year_month = df.groupBy("Year", "Month", "Traffic_Signal").agg(count("*").alias("AccidentCount"))
# Analyze accidents by city and sort by incident count
cityAggregated = df.groupBy("city") \
    .agg(count("*").alias("incident_count")) \
    .orderBy(col("incident_count").desc())

# Group by the "Traffic_Signal" column and calculate the count of accidents
accident_count_by_traffic_signal = df.groupBy("Traffic_Signal").agg(count("*").alias("AccidentCount"))

# Analyze accidents by state and severity
stateSeverityAggregated = df.groupBy("state", "severity_category") \
    .agg(count("*").alias("incident_count")) \
    .orderBy("state", "severity_category")

# Group by "Street," "City," and "State," and calculate the count of incidents
incident_count_by_location = df.groupBy("Street", "City", "State").agg(count("*").alias("IncidentCount")).orderBy(col("Incidentcount").desc())

#Count of Accidents by Day and Night
day_night_counts = df.groupBy("day_night").agg(count("*").alias("AccidentCount"))

#Average Severity of Accidents during Day and Night
severity_by_day_night = df.groupBy("day_night").agg(avg("Severity").alias("Avg_Severity"))

# Show the result
incident_count_by_location.show()
dayOfWeekAggregated.show()
cityAggregated.show()
stateSeverityAggregated.show()
accident_count_by_year_month.show()
day_night_counts.show()
severity_by_day_night.show()

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
    SELECT year, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY year
    ORDER BY year
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
    SELECT month, COUNT(*) AS accident_count
    FROM capstone_table
    GROUP BY month
    ORDER BY month
""")
accidents_by_month.show()

df.write.mode("overwrite").option("header", "true").saveAsTable("capstone.accident_new_trans")

from pyspark.sql import SQLContext 
from pyspark.sql.functions import isnan, when, count, col, length, desc, unix_timestamp, from_unixtime

sqlContext = SQLContext(sc)

# read csv into dataframes
df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
# Discard Columns
drop_list = ['Facility Type', 'School Name', 'School Number', 'School Region', 'School Code', 'School Phone Number', 'School Address', 'School City', 'School State', 'School Zip']
df = df.select([column for column in df.columns if column not in drop_list])

#Coerce invalid values to normal
df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^(\d{5}(-)?(\d{4})?|[A-Z]\d[A-Z] ?\d[A-Z]\d)$')== False, 'N/A').otherwise(df['Incident Zip']))

#Fill null values to N/A
df = df.fillna('N/A')


df = df.withColumn('year', col('Created Date').substr(7,4))
df = df.withColumn('month', col('Created Date').substr(0,2))
df = df.withColumn('day', col('Created Date').substr(0,10))

df.select('day',from_unixtime(unix_timestamp('day', 'MM/dd/yyy')).alias('date')).show()

# Count per day and sort by dates
df = df.withColumn('dayUnix',from_unixtime(unix_timestamp('day', 'MM/dd/yyy')))

df2 = df.groupBy('dayUnix').count().sort('dayUnix')
# Export results
df2.write.format('com.databricks.spark.csv').save('day.csv')


# Compaint types
complaint = df.groupBy('Complaint Type').count().sort(desc('count')).show()
complaint.write.format('com.databricks.spark.csv').save('complaint.csv')

location = df.groupBy('Location Type').count().sort(desc('count')).show()
location.write.format('com.databricks.spark.csv').save('location.csv')

# Closed Date - Created Date
df = df.withColumn('createdDay', col('Created Date').substr(0,10))
df = df.withColumn('closedDay', col('Closed Date').substr(0,10))

df = df.withColumn('createdUnix',unix_timestamp('createdDay', 'MM/dd/yyy'))
df = df.withColumn('closedUnix',unix_timestamp('closedDay', 'MM/dd/yyy'))
# Get the duration data
df = df.withColumn('periodUnix',(col('closedUnix')-col('createdUnix'))/86400)

# Count durations
df.groupBy('periodUnix').count().sort(desc('count')).show()
# Find extreme durations
df.select('periodUnix','createdDay','closedDay').sort(desc('periodUnix')).show()
# Discard results that closed after 2017
df.select('periodUnix','createdDay','closedDay','closedUnix').sort(desc('periodUnix')).where(col('closedUnix')<1514764800).show()




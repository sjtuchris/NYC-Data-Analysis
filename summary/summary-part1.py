from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, length, desc, unix_timestamp, from_unixtime

sqlContext = SQLContext(sc)

# read csv into dataframes
df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Group By Year
df.withColumn('year', col('Created Date').substr(7,4)).groupBy('year').count().show()

# Group By Month
df.withColumn('month', col('Created Date').substr(0,2)).groupBy('month').count().show()

# Group By Zip
df.groupBy('Incident Zip').count().sort('count',ascending=False).show()

# Group By Closed Year
df.withColumn('year_close', col('Closed Date').substr(7,4)).groupBy('year_close').count().show()

# Add Date column
df = df.withColumn('date', col('Created Date').substr(0,10))
df = df.withColumn('date_close', col('Closed Date').substr(0,10))

# Get Daily Average Create
for x in range(2009,2018):
  df.filter(col('year')==str(x)).groupBy('date').count().describe().show()

# Get Daily Average Close
for x in range(2009,2018):
    df.filter(col('year_close')==str(x)).groupBy('date_close').count().filter(length(col('date_close'))==10).agg(avg(col('count'))).show()

# Get Most Complaint Type
for x in range(2009,2018):
    df.filter(col('year')==str(x)).groupBy('Complaint Type').count().sort('count', ascending=False).take(1)

# Get Most Complaint Zip Area
for x in range(2009,2018):
    df.filter(col('year')==str(x)).groupBy('Incident Zip').count().sort('count', ascending=False).take(2)

# Get Borough
for x in range(2009,2018):
    df.filter(col('year')==str(x)).groupBy('Borough').count().sort('count', ascending=False).take(2)

for x in range(2009,2018):
    df.filter(col('year')==str(x)).groupBy('Agency').count().sort('count', ascending=False).take(2)

for x in range(2009,2018):
    df.filter(col('year')==str(x)).groupBy('Location Type').count().sort('count', ascending=False).take(2)

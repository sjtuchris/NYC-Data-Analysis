from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, length

sqlContext = SQLContext(sc)

# read csv into dataframes
df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
# count number of rows
df.count()

# describe the rows, see numeric min/max values for any numerical illegal values
df.describe().show()

# check None values in each row
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).take(1)
# check N/A values in each row
df.select([count(when(col(c) == 'N/A', c)).alias(c) for c in df.columns]).take(1)
# check unspecified values in each row
df.select([count(when(col(c)=='Unspecified', c)).alias(c) for c in df.columns]).take(1)
# Community Board column contains '0 Unspecified'
df.select([count(when(col(c)=='0 Unspecified', c)).alias(c) for c in df.columns]).take(1)

# Group by creation Date to see abnormal
df.groupBy('Created Date').count().describe().show()
df.groupBy('Closed Date').count().describe().show()

# Group by to detect suspicious data
df.groupBy('Agency').count().sort('count').show()
df.groupBy('Agency Name').count().sort('count').show()
df.groupBy('Complaint Type').count().sort('count').show()
df.groupBy('Address Type').count().sort('count').show()
df.groupBy('City').count().sort('count').show()
df.groupBy('Facility Type').count().sort('count').show()
df.groupBy('Borough').count().sort('count').show()
df.groupBy('Park Borough').count().sort('count').show()

# Take a look at the suprisingly high occurance
df.groupBy('Closed Date').count().orderBy('count',ascending=False).take(1)

df.select(col('Incident Zip')).filter(col('Incident Zip').rlike('^(\d{5}(-)?(\d{4})?|[A-Z]\d[A-Z] ?\d[A-Z]\d)$')== False).show()

# Check abnormal Zip Code
df.where(length(col('Incident Zip')) > 0).select(col('Incident Zip')).filter(col('Incident Zip').rlike('^(\d{5}(-)?(\d{4})?|[A-Z]\d[A-Z] ?\d[A-Z]\d)$')==False).groupBy('Incident Zip').count().show()

# Discard Columns
drop_list = ['Facility Type', 'School Name', 'School Number', 'School Region', 'School Code', 'School Phone Number', 'School Address', 'School City', 'School State', 'School Zip']
df = df.select([column for column in df.columns if column not in drop_list])

#Coerce invalid values to normal
df = df.withColumn('Incident Zip', when(col('Incident Zip').rlike('^(\d{5}(-)?(\d{4})?|[A-Z]\d[A-Z] ?\d[A-Z]\d)$')== False, 'N/A').otherwise(df['Incident Zip']))

#Fill null values to N/A
df = df.fillna('N/A')

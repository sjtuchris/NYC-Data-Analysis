from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, length, avg

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

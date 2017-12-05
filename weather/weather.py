from datetime import datetime
from pyspark.sql.functions import col, udf

df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Read Weather Dataset
df_w = sqlContext.read.load('weather.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Transform time format to string corresponding to existing dataset
func =  udf (lambda x: datetime.strptime(x, '%d-%m-%Y').strftime('%m/%d/%Y'))
df_w = df_w.withColumn('day', func(col('date')))

# GroupBy Day
df_2016_count = df_2016.groupBy('day').count()

# Join the weather table
df_2016_count.join(df_w, df_2017_count['day'] == df_w['day']).sort('count',ascending=False).show()
count_temp_joined = df_2016_count.join(df_w, df_2016_count['day'] == df_w['day']).sort('count',ascending=False)

# Filter heating complaints
df_2016_h_count = df_2016.filter(col('Complaint Type')=='HEAT/HOT WATER').groupBy('day').count()
h_count_temp_joined = df_2016_h_count.join(df_w, df_2016_count['day'] == df_w['day']).sort('count',ascending=False)

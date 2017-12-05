# Read Weather Dataset
df_w = sqlContext.read.load('weather.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

from datetime import datetime
from pyspark.sql.functions import col, udf

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

# Read
df_edu = sqlContext.read.load('degree_original.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
df_income = sqlContext.read.load('income_original.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

df_count_zip = df.groupBy('Incident Zip').count()

# Join education level
zip_edu_joined = df_count_zip.join(df_edu, df_count_zip['Incident Zip'] == df_edu['zip'])

# Join income level
zip_income_joined = df_count_zip.join(df_income, df_count_zip['Incident Zip'] == df_income['zip'])

# Join all
all_joined = zip_edu_joined.join(df_income,  zip_edu_joined['Incident Zip'] == df_income['zip'])

# Get education index
all_joined = all_joined.withColumn('edu_index', all_joined['degree']/all_joined['household'])

# Save to csv for visualtion
zip_edu_joined.save('zip_edu','com.databricks.spark.csv')
zip_income_joined.save('zip_income','com.databricks.spark.csv')
all_joined.save('zip_edu2','com.databricks.spark.csv')

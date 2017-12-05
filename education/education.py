from datetime import datetime
from pyspark.sql.functions import col

df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Read education & income dataset
df_edu = sqlContext.read.load('degree_original.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
df_income = sqlContext.read.load('income_original.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

df_count_zip = df.groupBy('Incident Zip').count()

# Join education level
zip_edu_joined = df_count_zip.join(df_edu, df_count_zip['Incident Zip'] == df_edu['zip'])

# Join all
all_joined = zip_edu_joined.join(df_income,  zip_edu_joined['Incident Zip'] == df_income['zip'])

# Get education index
all_joined = all_joined.withColumn('edu_index', all_joined['degree']/all_joined['household'])

# Save to csv for visualtion
zip_edu_joined.save('zip_edu','com.databricks.spark.csv')
all_joined.save('zip_edu2','com.databricks.spark.csv')

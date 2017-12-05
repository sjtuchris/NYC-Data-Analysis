from datetime import datetime
from pyspark.sql.functions import col

df = sqlContext.read.load('20*.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

# Read income dataset
df_income = sqlContext.read.load('income_original.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')

df_count_zip = df.groupBy('Incident Zip').count()

# Join income level
zip_income_joined = df_count_zip.join(df_income, df_count_zip['Incident Zip'] == df_income['zip'])

# Save to csv for visualtion
zip_income_joined.save('zip_income','com.databricks.spark.csv')

# read csv into dataframes
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
# df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("2009.csv")

df = sqlContext.read.load('2009.csv',
                          format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')
# count number of rows
df.count()

# describe the rows, see numeric min/max values for any numerical illegal values
df.describe().show()

from pyspark.sql.functions import isnan, when, count, col
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

import sys
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json



def process_name(df):
  for col_name in df.columns:
    if '_airbyte_data_data_' in col_name or '_airbyte_data_' in col_name:
      new_col_name = col_name.replace('_airbyte_data_data_', '').replace('_airbyte_data_', '')
      df = df.withColumnRenamed(col_name, new_col_name)
  return df  

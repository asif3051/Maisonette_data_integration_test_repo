import sys
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json



def flatten_str(spark, df, col_name):
    """
    Flatten the struct like string to a proper pyspark struct
    :param spark: spark session
    :param df: dataframe to flatten
    :param col_name: column name of string type column
    :return: struct like string converted to string
    """
    # Trying to load any non-null value of string column as dictionary
    # If json.loads fails then the column is actually a string not a struct
    # else it is a struct then we need to parse it using from_json method
    remove_column = True
    try:
        json.loads(eval(f'df.filter(df.{col_name}.isNotNull()).head().{col_name}')).keys()
    except:
        return df, remove_column
    remove_column = False
    json_schema = spark.read.json(df.rdd.map(lambda row: eval(f'row.{col_name}'))).schema
    df = df.withColumn(f'{col_name}_struct', from_json(col(col_name), json_schema))
    df = df.drop(col_name)
    df = df.withColumnRenamed(f'{col_name}_struct', col_name)
    return df, remove_column

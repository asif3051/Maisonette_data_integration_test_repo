import sys
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json



def flatten_struct(df, col_name, col_schema):
    """
    Flatten the schema of struct
    :param df: dataframe object
    :param col_name: column of struct type
    :param col_schema: internal schema of struct
    :return: flattened struct
    """
    expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) 
        for k in [n.name for n in col_schema]]
    df = df.select("*", *expanded).drop(col_name)
    return df

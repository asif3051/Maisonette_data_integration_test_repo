import sys
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json




def flatten_array(df, col_name, col_list=None):
    """
    Flatten/explode the array column-wise or row-wise
    :param df: dataframe to flatten
    :param col_name: column of array type
    :param col_list: list of columns to be exploded column wise
    :return: flattened array
    """
    remove_column = False
    if col_list is None:
        col_list = []
    if col_name in col_list:
        # exploding column wise directly if is not an array of struct
        remove_column = True
        if not isinstance(df.schema[col_name].dataType.elementType, StructType):
            df = df.select("*", *[(col(f"{col_name}")[x]).alias(f'{col_name}_other_{x + 1}')
                for x in range(0, 4)])
            df = df.drop(f'{col_name}')
        else:
            # parse invalid key if it is present otherwise send respective exception
            try:
                df = df.withColumn(f'valid_{col_name}', 
                    expr(f"filter({col_name}, x -> x.invalid = False)"))
            except:
                raise exceptions.FlatteningArrayOfStructIsInvalidKeyMissing

            # if invalid key is present parse isPrimary key
            try:
                df = df. \
                    withColumn(f"{col_name}_primary",
                        expr(f"filter(valid_{col_name}, x -> x.isPrimary = True)")). \
                    withColumn(f"{col_name}_other",
                        expr(f"filter(valid_{col_name}, x -> x.isPrimary = False)")). \
                    drop(col_name, f'valid_{col_name}')
                df = df.select("*",
                               *[(col(f"{col_name}_other")[x]).alias(f'{col_name}_other_{x + 1}')
                               for x in range(0, 4)])
                df = df.drop(f'{col_name}_other')
            except:
                raise exceptions.FlatteningArrayOfStructIsPrimaryKeyMissing
    else:
        # exploding row wise
        df = df.withColumn(col_name, explode_outer(col_name))
    return df, remove_column


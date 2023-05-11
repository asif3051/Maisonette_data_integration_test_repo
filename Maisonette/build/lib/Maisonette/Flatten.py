import sys
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from Maisonette.PrepareComplexFields import prepare_complex_fields
from Maisonette.FlattenStruct import flatten_struct 
from Maisonette.FlattenArray import flatten_array
from Maisonette.FlattenStr import flatten_str


def flatten(spark, df, col_list=None):
    """
    Driver function for flattening a data frame containing columns of various datatypes
    :param df: data frame to flatten
    :param spark: spark session
    :param col_list: list of columns to be exploded column wise
    :return: flattened dataframe
    """
    complex_fields = prepare_complex_fields(df)
    columns_to_remove = []
    if col_list is None:
        col_list = []
    while len(complex_fields) != 0:
        remove_column = False
        col_name = list(complex_fields.keys())[0]
        if isinstance(complex_fields[col_name], StructType):
            df = flatten_struct(df, col_name, complex_fields[col_name])
        elif isinstance(complex_fields[col_name], ArrayType):
            try:
                df, remove_column = flatten_array(df, col_name, col_list)
            except:
                raise  # forward the exception raised in previous step
        else:
            df, remove_column = flatten_str(spark, df, col_name)
        if remove_column:
            columns_to_remove.append(col_name)
        complex_fields = prepare_complex_fields(df, columns_to_remove)
    return df


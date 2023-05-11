import sys
import json
from pyspark.sql.functions import explode_outer
from pyspark.sql.types import StructType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json




def prepare_complex_fields(df, columns_to_remove=None):
    """
    utils function for preparing a dictionary of complex fields 
    i.e. of datatype - Struct, String, Array
    :param df: dataframe object
    :param columns_to_remove: list of columns to be removed from complex fields
    :return: dict of complex fields
    """
    if columns_to_remove is None:
        columns_to_remove = []
    return dict([(field.name, field.dataType) for field in df.schema.fields
                if (field.name not in columns_to_remove) 
                and (isinstance(field.dataType, ArrayType)
                or isinstance(field.dataType, StructType)
                or isinstance(field.dataType, StringType))])


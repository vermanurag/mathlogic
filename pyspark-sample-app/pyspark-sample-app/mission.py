import pyspark.sql.functions as F

def myFunc(df):
    return df.withColumn("life_goal", F.lit("escape!"))

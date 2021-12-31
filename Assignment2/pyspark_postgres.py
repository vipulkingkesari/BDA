"""
This moudle is to write data into PostgresSQL db
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

#Connection details
PSQL_SERVERNAME = "localhost"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "mydb"
PSQL_USRRNAME = "myuser"
PSQL_PASSWORD = "mypass"

URL = f"jdbc:postgresql://{PSQL_SERVERNAME}/{PSQL_DBNAME}"

#Table details
TABLE_MYTABLE = "t1"
TABLE_EMPLOYEE = "Employee"

def get_employee(_spark):
    """
    This method is to read people.json into a dataframe and return to the caller
    """
    _df_people = spark.read \
        .format("json") \
        .load("C:\\Spark\\spark-2.4.6-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    _df_people.show()

    return _df_people

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PostgrsSQL demo") \
        .getOrCreate()

    df_people = get_employee(spark)

    df_people.write\
        .format("jdbc")\
        .option("url", URL)\
        .option("dbtable", TABLE_EMPLOYEE)\
        .option("user", PSQL_USRRNAME)\
        .option("password", PSQL_PASSWORD)\
        .mode("append")\
        .save()

    df_employee = spark.read\
        .format("jdbc")\
        .option("url", URL)\
        .option("dbtable", TABLE_EMPLOYEE)\
        .option("user", PSQL_USRRNAME)\
        .option("password", PSQL_PASSWORD)\
        .load()

    df_employee.where("age >= 20 and age <= 50")\
        .withColumn("retirement_age", col("age")+35)\
        .groupBy(col("age"))\
        .agg({"age":"sum"})\
        .select("age", col("sum(age)").alias("total_age"))\
        .show()

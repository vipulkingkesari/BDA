from pyspark.sql import SparkSession
import time
# take data in dataframe and then compute query on it

begin = time.time()
#Connection details
PSQL_SERVERNAME = "localhost:5432"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "postgres"
PSQL_USRRNAME = "postgres"
PSQL_PASSWORD = "bigdata"

URL = f"jdbc:postgresql://{PSQL_SERVERNAME}/{PSQL_DBNAME}"

#Table details
TABLE_EMPLOYEE = "PUBLIC.pull_request"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PostgrsSQL demo") \
        .config("spark.executor.instances",1) \
        .getOrCreate()

    df_employee = spark.read\
        .format("jdbc")\
        .option("url", URL)\
        .option("dbtable", TABLE_EMPLOYEE)\
        .option("user", PSQL_USRRNAME)\
        .option("password", PSQL_PASSWORD)\
        .load()
    df_employee.createOrReplaceTempView("pull_requests")

    #df2 = spark.sql("select date,event,count(pull_requestid) From pull_requests Group By event,date Having event='opened' Order by date")   #1a      

    #df2 = spark.sql("select date,event,count(pull_requestid) From pull_requests Group By event,date Having event='discussed' Order by date")  #1b

    #df2 = spark.sql("select date_time,MAX(number_of_times) from (select author, date_part('month',date) date_time ,count(pull_requestid) as number_of_times from pull_requests where event='discussed' group by author, date_time) as deliveer group by date_time order by date_time") #2

    #df2 = spark.sql("select date_time ,MAX(number_of_times) from (select author, date_part('week',date) date_time ,count(pull_requestid) as number_of_times from pull_requests where event='discussed' group by author, date_time) as deliveer group by date_time order by date_time") #3

    #df2 = spark.sql("SELECT date_trunc('week',date) weeks,count(pull_requestid) from pull_requests group by event,weeks having event='opened' order by weeks") #4

    #df2 = spark.sql("select date_trunc('month',date) months,count(pull_requestid) from pull_requests where event='merged' and EXTRACT(YEAR FROM date)='2010' group by months order by months") #5

    df2 = spark.sql("select date_trunc('day',date) date,count(event) from pull_requests group by date order by date") #6

    #df2 = spark.sql("select distinct(author) ,count(*) as countpr from pull_requests where event='opened' and EXTRACT(YEAR FROM date)='2011' group by author order by countpr desc") #7
    

    df2.printSchema()
    df2.show()

    time.sleep(1) 
    # store end time 
    end = time.time() 
      
    # total time taken 
    print(f"Total runtime of the program is {end - begin}") 



#from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SparkSession
from datetime import date
# import findspark
# findspark.init("/hadoop/spark")
import pyspark
if __name__ == "__main__":
    #spark=SparkSession.builder.enableHiveSupport().appName('adjuster').getOrCreate()
    spark=SparkSession.builder.appName('adjuster').getOrCreate()
    #conf = SparkConf().setAppName("adjuster")
    #sc = SparkContext(conf=conf)
    #sqlContext=HiveContext(sc)
    add_file='/home/hadoop/data/add.csv'
    data='/home/hadoop/data/final_data.csv'
    data=spark.read.csv(data,inferSchema=True,header=True)
    schema=data.schema
    results=data.agg({'stud_id':'count'})
    next_id=results.collect()[0].asDict()["count(stud_id)"]+1
    new_rows=[]
    add_file=spark.read.csv(add_file,inferSchema=True,header=True)
    if "first_name" in add_file.columns and "middle_name" in add_file.columns and "last_name" in add_file.columns:
        add_file=add_file.collect()
        for i in range(len(add_file)):
            my_dict=add_file[i].asDict()
            sid=next_id
            next_id+=1
            first_name=my_dict['first_name']
            middle_name=my_dict['middle_name']
            last_name=my_dict['last_name']
            valid_from=str(date.today())
            valid_to='till date'
            latest=True
            version_no=1
            new_rows.append((sid,first_name,middle_name,last_name,valid_from,valid_to,latest,version_no))
        if len(new_rows)>0:
            newRow = spark.createDataFrame(new_rows,schema)
            data = data.union(newRow)
    data.coalesce(1).write.option('header',True).mode('overwrite').csv('/home/hadoop/data/final_data1')





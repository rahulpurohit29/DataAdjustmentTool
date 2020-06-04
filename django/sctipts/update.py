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
    update='/home/hadoop/data/update.csv'
    data='/home/hadoop/data/final_data.csv'
    updated_ids=[]
    data=spark.read.csv(data,inferSchema=True,header=True)
    schema=data.schema
    #data.createOrReplaceTempView('table')
    update=spark.read.csv(update,inferSchema=True,header=True)
    if "stud_id" in update.columns and "first_name" in update.columns and "middle_name" in update.columns and "last_name" in update.columns:
        update=update.collect()
        for i in range(0,len(update)):
            #data.createOrReplaceTempView('table')
            new_rows=[]
            update_dict=update[i].asDict()
            sid=update_dict['stud_id']
            updated_ids.append(sid)
            results=data.filter(data.stud_id==sid).filter(data.latest=="True")
            #results.show()
            if len(results.collect())>0:
                old_dict=results.collect()[0].asDict()
                old_first_name=old_dict['first_name']
                old_middle_name=old_dict['middle_name']
                old_last_name=old_dict['last_name']
                old_valid_from=old_dict['valid_from']
                old_valid_to=str(date.today())
                old_version_no=old_dict['version_no']
                latest=False
                new_rows.append((sid,old_first_name,old_middle_name,old_last_name,old_valid_from,old_valid_to,latest,old_version_no))

                new_first_name=update_dict['first_name']
                new_middle_name=update_dict['middle_name']
                new_last_name=update_dict['last_name']
                new_valid_from=str(date.today())
                new_valid_to='till date'
                latest=True
                new_version_no=old_version_no+1
                new_rows.append((sid,new_first_name,new_middle_name,new_last_name,new_valid_from,new_valid_to,latest,new_version_no))
                data=data.union(results).subtract(data.intersect(results))
                if len(new_rows)>0:
                    newRow = spark.createDataFrame(new_rows,schema)
                    #newRow.show()
                    data = data.union(newRow)
    #data.toPandas().to_csv("final_data.csv",header=True,index=False)
    #data.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/home/hadoop/data/final_data.csv')
    data.coalesce(1).write.option('header',True).mode('overwrite').csv('/home/hadoop/data/final_data1')

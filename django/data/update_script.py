import findspark
findspark.init("C:\\spark-2.4.5-bin-hadoop2.7")
import pyspark
from datetime import date
# importing the Libraries

# Adding new entries
def add_entries(data,add_file):
    new_rows=[]
    data=spark.read.csv(data,inferSchema=True,header=True)
    schema=data.schema
    data.createOrReplaceTempView('table')
    results=spark.sql('SELECT count(stud_id) FROM table')
    next_id=results.collect()[0].asDict()["count(stud_id)"]+1
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
    return data


# Deleting entires
def del_entries(data,ids):
    data=spark.read.csv(data,inferSchema=True,header=True)
    for id in ids:
        data=data.filter(f"stud_id!={id}")
    return data


# Updating the entires
def update_entries(data,update):
    data=spark.read.csv(data,inferSchema=True,header=True)
    schema=data.schema
    data.createOrReplaceTempView('table')
    update=spark.read.csv(update,inferSchema=True,header=True)
    if "stud_id" in update.columns and "first_name" in update.columns and "middle_name" in update.columns and "last_name" in update.columns:
        update=update.collect()
        for i in range(0,len(update)):
            data.createOrReplaceTempView('table')
            new_rows=[]
            update_dict=update[i].asDict()
            sid=update_dict['stud_id']
            results=spark.sql(f'select * from table where stud_id={sid} and latest=True')
            results.show()
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
                    newRow.show()
                    data = data.union(newRow)
    return data

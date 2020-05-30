def update_entries(data,update):
    global updated_ids
    updated_ids=[]
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
            updated_ids.append(sid)
            results=spark.sql(f'select * from table where stud_id={sid} and latest=True')
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
    data.createOrReplaceTempView('table')
    data=spark.sql("SELECT * FROM table ORDER BY stud_id")
    # print(ids)
    # for id in ids:
    #     results=spark.sql(f'SELECT * FROM table WHERE stud_id={id}')
    #     results.show()
        #data.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('C:\\Users\\Administrator\\Desktop\\DataAdjustmentTool\\django\\data\\'+str(date.today())+'_output.csv')
    return data
def add_entries(data,add_file):
    global updated_ids
    updated_ids=[]
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
            updated_ids.append(sid)
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
    data.createOrReplaceTempView('table')
    data=spark.sql("SELECT * FROM table ORDER BY stud_id")
    # print(ids)
    # for id in ids:
    #     results=spark.sql(f'SELECT * FROM table WHERE stud_id={id}')
    #     results.show()
        #data.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('C:\\Users\\Administrator\\Desktop\\DataAdjustmentTool\\django\\data\\'+str(date.today())+'_output.csv')
    return data
from pyspark.shell import sqlContext
from pyspark.sql.functions import array, explode, col
from pyspark.sql import functions as f
from pyspark.sql.types import DateType
import datetime

def parse (line):
    try:
        if(line):            
            timestamp = line.timestamp0[:8]
            week = line.timestamp0[:4] + datetime.date(int(line.timestamp0[:4]), int(line.timestamp0[4:6]), int(line.timestamp0[6:8])).strftime("%V")
            calledno = line.calledno[2:4]
            callingno = line.callingno[2:4]
            calduration = round(line.callduration / 60.0, 2)
            isCompetetorNum = 1 if line.calledno in hotline_list else 0
            if callingno == '71' or callingno == '70':                
                if calledno == '71' or calledno == '70' :
                    return (line.callingno[2:], line.calledno[2:], 'in', timestamp, week, calduration, isCompetetorNum)         
                else:
                    return (line.callingno[2:], line.calledno[2:], 'out', timestamp, week, calduration, isCompetetorNum)
            else:                
                return (line.calledno[2:], line.callingno[2:], 'out', timestamp, week, calduration, isCompetetorNum)
        else:
            return(line, '', 'invalid', '', 'Empty', '', 0)
    except TypeError:       
        return (line, '', 'invalid', '', 'TypeError', '', 0)
    except ValueError:
        return (line, '', 'invalid', '', 'ValueError', '', 0)
    except:
        return (line, '', 'invalid', '', 'UnexpectedError', '', 0)
    
def nafill (line):
    if(line):
        if(line.number_in):
            number = line.number_in
        else:
            number = line.number_out
    return (number, line.coefficiant_of_variance_in, line.coefficiant_of_variance_out, line.call_count_in, line.call_count_out, line.call_count_competitor_out)

if __name__ == '__main__':
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    df = sqlContext.sql("SELECT * FROM cdrdb.pre_rec_cdr_pqt_vw")
    hotline = sqlContext.read.text('/data/resources/numlist.txt')
    global hotline_list 
    hotline_list = hotline.map(lambda x: x.value).collect()
    df1 = df.select('year', 'month').groupBy('year', 'month').count()
    partionList = df1.select('year', 'month').collect()
    temp = True
    for i, x in enumerate(partionList):        
        print(x)
        if(x.year and x.month):
            temp_df = sqlContext.sql("SELECT * FROM cdrdb.pre_rec_cdr_pqt_vw WHERE year='{}' AND month='{}'".format(str(x.year), str(x.month)))
            df2 = sqlContext.createDataFrame(temp_df.map(parse), ['number', 'number2', 'type', 'date', 'week', 'callduration', 'iscompethot'])
            if(temp):
                temp = False
                print("Overwriting")
                df2.filter(df2.type != 'invalid').write.mode("overwrite").saveAsTable("cdr_step0", format="parquet", path="/data/intermediate_data/cdr_step0/")
            else:
                print("Appending")
                df2.filter(df2.type != 'invalid').write.mode("append").saveAsTable("cdr_step0", format="parquet", path="/data/intermediate_data/cdr_step0/")
    
    cleanedDF = sqlContext.sql("SELECT * FROM default.cdr_step0")
    df_in = cleanedDF.filter(cleanedDF.type == "in").withColumn("s3", array(cleanedDF.number, cleanedDF.number2)).select(col('s3'), col('number'), col('type'), col('date'), col('week'), col('callduration'), col('iscompethot')).withColumn("number", explode(col('s3'))).drop(col("s3"))
    comp_df = cleanedDF.filter(cleanedDF.type != "in").select(cleanedDF.number, cleanedDF.type, cleanedDF.date, cleanedDF.week, cleanedDF.callduration, cleanedDF.iscompethot).unionAll(df_in)
    comp_df.write.mode("overwrite").saveAsTable("cdr_step1", format="parquet", path="/data/intermediate_data/cdr_step1/")
    
    df = sqlContext.read.parquet("/data/intermediate_data/cdr_step1/")
    df1 = df.sort('number').groupBy('number', 'type', 'week').agg(f.sum('callduration').alias('call_sum'), f.count('callduration').alias('call_count'), f.sum('iscompethot').alias('call_count_competitor'))
    df1.write.mode("overwrite").saveAsTable("cdr_step2", format="parquet", path="/data/intermediate_data/cdr_step2/")
    
    df = sqlContext.read.parquet('/data/intermediate_data/cdr_step2/')
    df1 = df.groupBy('number', 'type').agg((f.stddev_pop('call_sum') / f.mean('call_sum')).alias('coefficiant_of_variance'), f.sum('call_count').alias('call_count'), f.sum('call_count_competitor').alias('call_count_competitor'))
    df1.write.mode("overwrite").saveAsTable("cdr_step3", format="parquet", path="/data/intermediate_data/cdr_step3/")
        
    df = sqlContext.read.parquet('/data/intermediate_data/cdr_step3/')
    df_out = df.where(df.type == "out").select(df.number, df.coefficiant_of_variance, df.call_count, df.call_count_competitor)
    df_in = df.where(df.type == "in").select(df.number, df.coefficiant_of_variance, df.call_count, df.call_count_competitor)
    df_out = df_out.withColumnRenamed('coefficiant_of_variance', 'coefficiant_of_variance_out')
    df_out = df_out.withColumnRenamed('call_count', 'call_count_out')
    df_out = df_out.withColumnRenamed('number', 'number_out')
    df_out = df_out.withColumnRenamed('call_count_competitor', 'call_count_competitor_out')
    df_in = df_in.withColumnRenamed('coefficiant_of_variance', 'coefficiant_of_variance_in')
    df_in = df_in.withColumnRenamed('call_count', 'call_count_in')
    df_in = df_in.withColumnRenamed('number', 'number_in')
    df_in = df_in.withColumnRenamed('call_count_competitor', 'call_count_competitor_in')
    df0 = df_in.join(df_out, df_in.number_in == df_out.number_out, 'outer')    
    df1 = sqlContext.createDataFrame(df0.map(nafill), ['number', 'coefficiant_of_variance_in', 'coefficiant_of_variance_out', 'call_count_in', 'call_count_out', 'call_count_competitor'])
    df1.filter(df1.number != 'null').na.fill({'coefficiant_of_variance_in':0, 'coefficiant_of_variance_out':0, 'call_count_in':0, 'call_count_out':0, 'call_count_competitor':0}).write.mode("overwrite").saveAsTable("cdr_step4", format="parquet", path="/data/intermediate_data/cdr_step4/")
    
    df = sqlContext.read.parquet('/data/intermediate_data/cdr_step4/')
    df_user = sqlContext.read.parquet('/data/db_files/mobisdb/smgt004_pqt/')
    df1 = df_user.withColumn("connected_on", df_user.connected_date.cast(DateType())).drop('connected_date')
    df2 = df1.groupBy('mobile_no').agg(f.max('connected_on').alias('connected_on'))
    cond = [df1.mobile_no == df2.mobile_no, df1.connected_on == df2.connected_on]
    df3 = df2.join(df1, cond, 'inner').drop(df1.mobile_no).drop(df1.connected_on)
    df4 = df3.withColumn('churned', f.when(df3.disconnected_on != 'null', 1).otherwise(0)).withColumn('number', df3.mobile_no.substr(2, 9)).drop('disconnected_on').drop('mobile_no')
    
    df5 = df.join(df4, df.number == df4.number, 'inner').select(df.number, df.coefficiant_of_variance_in, df.coefficiant_of_variance_out, df.call_count_in, df.call_count_out, df.call_count_competitor, df4.churned)
    df5.write.mode("overwrite").saveAsTable("cdr_step5_1", format="parquet", path="/data/intermediate_data/cdr_step5_1/")
    
    
    

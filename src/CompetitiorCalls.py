from pyspark.shell import sqlContext
import datetime

def parse (line):
    try:
        if(line):
            if(len(line.callingno) != 11 or len(line.calledno) != 11):
                return (line.callingno, line.calledno, 'invalid', line.timestamp0, 'InvalidNumber', line.callduration)
            else:
                timestamp = line.timestamp0[:8]
                week = line.timestamp0[:4] + datetime.date(int(line.timestamp0[:4]), int(line.timestamp0[4:6]), int(line.timestamp0[6:8])).strftime("%V")
                calledno = line.calledno[2:4]
                callingno = line.callingno[2:4]
                calduration = round(line.callduration / 60.0, 2)
                if callingno == '71' or callingno == '70':
                    if calledno == '71' or calledno == '70' :
                        return (line.callingno[2:], line.calledno[2:], 'in', timestamp, week, calduration)         
                    else:
                        return (line.callingno[2:], '', 'out', timestamp, week, calduration)
                else:
                    if calledno == '71' or calledno == '70' :
                        return (line.calledno[2:], '', 'out', timestamp, week, calduration)
                    else:
                        return (line.callingno[2:], line.calledno[2:], 'invalid', line.timestamp0, 'invalid', line.callduration)
        else:
            return(line, '', 'invalid', '', 'Empty', '')
    except TypeError:       
        return (line, '', 'invalid', '', 'TypeError', '')
    except ValueError:
        return (line, '', 'invalid', '', 'ValueError', '')
    except:
        return (line, '', 'invalid', '', 'UnexpectedError', '')

if __name__ == '__main__':    
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    df = sqlContext.sql("SELECT * FROM cdrdb.pre_rec_cdr_pqt_vw")
    df1 = df.select('year', 'month').groupBy('year', 'month').count()
    partionList = df1.select('year', 'month').collect()
    temp = True
    for i, x in enumerate(partionList):        
        print(x)
        if(x.year and x.month):
            temp_df = sqlContext.sql("SELECT * FROM cdrdb.pre_rec_cdr_pqt_vw WHERE year='{}' AND month='{}'".format(str(x.year), str(x.month)))
            df2 = sqlContext.createDataFrame(temp_df.map(parse), ['number', 'number2', 'type', 'date', 'week', 'callduration'])
            if(temp):
                temp = False
                print("Overwriting")
                df2.filter(df2.type != 'invalid').write.mode("overwrite").saveAsTable("cdr_step0", format="parquet", path="/data/intermediate_data/cdr_step0/")
            else:
                print("Appending")
                df2.filter(df2.type != 'invalid').write.mode("append").saveAsTable("cdr_step0", format="parquet", path="/data/intermediate_data/cdr_step0/")

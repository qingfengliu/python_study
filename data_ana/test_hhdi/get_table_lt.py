import xlrd
import csv
from sqlalchemy import create_engine
import datetime
starttime = datetime.datetime.now()


dizhi_dict={'dm'}
data = xlrd.open_workbook(r'C:\Users\50477\Desktop\HHID检查DM层数据库.xlsx')
table = data.sheets()[0]
oracle_table=table.col_values(0)[1:]
# print(oracle_table)
temp=[]
for tmp in oracle_table:
    if tmp.split('_')[0].lower()=='ods':
        temp.append(['ODS_保密',tmp])
    elif tmp.split('_')[0].lower()=='dm':
        temp.append(['XXXX', tmp])
    # else:
    #     temp.append(['RP_保密', tmp])
data=[]
for i in temp:
    engine = create_engine(
        'oracle+cx_oracle://XXXX:XXXX123@IP保密:1521/ORCL?charset=utf8')
    db_conn = engine.connect()
    sql='select max(loadtime) from '+'%s.%s' %(i[0],i[1])
    try:
        jiegou_infos=db_conn.execute(sql)
        for tmp in jiegou_infos:
            tmp=tmp[-1]
            if tmp:
                tmp=tmp.strftime("%Y-%m-%d %H:%M:%S")
            else:
                tmp='NO MAX LOADTIME ERROR'
            data.append([i[1],tmp])
        db_conn.close()
    except Exception as e:
        data.append([i[1],'NO LOADTIME COL ERROR'])

with open('测试结果.csv','w',newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['表明', 'update字段'])
    for i in data:
        writer.writerow([i[0],i[1]])
endtime = datetime.datetime.now()
print((endtime - starttime).seconds)
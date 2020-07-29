import argparse
import pandas as pd
def get_attr():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--path",help="存放存储过程的文件夹")
    args = parser.parse_args()
    try :
        path=args.path
    except Exception as e:
        raise Exception("参数错误")
    return path

if False:
    path=get_attr()
    files=[]
    for filename in os.listdir(path):
        files.append(path+'\\'+filename)
    csvFile2 = open('对照表.csv','w', newline='') # 设置newline，否则两行之间会空一行
    writer = csv.writer(csvFile2)
    writer.writerow(['文件名','查询名','查询语句'])
    for file in files:
        htm=etree.parse(file)
        filename=file.split('\\')[-1]
        tables=htm.xpath('//TableDataMap/TableData')
        for table in tables:
            name=table.xpath('@name')[0]
            try:
                query=table.xpath('Query/text()')[0]
            except Exception as e:
                print(file,name)
                query=''
            writer.writerow([filename,name,query])
    csvFile2.close()

######################生产表#############################


data_frame1 = pd.read_csv('对照表.csv',encoding='gbk')
data_frame2 = pd.read_excel('模板对照表.xlsx')
data_frame2 = data_frame2.fillna(method='pad')
data_tmp = data_frame2[['分析主题','模板名称']].drop_duplicates()
data_tmp2 = data_frame2[['一级目录','二级目录','三级目录','页面','分析主题']].drop_duplicates()
data_tmp3=pd.merge(data_tmp2,data_tmp).drop_duplicates()
data_tmp3['弹窗']=data_tmp3['分析主题']
data_tmp3=data_tmp3.copy()
data_frame2['模板名称']=data_frame2['模板名称.1']
del data_frame2['模板名称.1']
data_tmp=pd.concat([data_frame2,data_tmp3],sort=False)
data_tmp=pd.merge(data_tmp,data_frame1,how='left',left_on='模板名称',right_on='文件名')
data_tmp=data_tmp.copy()
data_tmp.to_csv('合成表.csv',encoding='gbk')
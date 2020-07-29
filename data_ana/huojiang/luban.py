import pandas as pd
data=pd.read_excel(r'E:\xxxx\鲁班奖.xlsx')
data_tmp1=data[data['参建单位'].notnull()]
data_tmp2=data[data['承建单位'].notnull()]
data_tmp1=data_tmp1[['序号','年度','发文文号','工程项目','参建单位','备注']]
data_tmp2=data_tmp2[['序号','年度','发文文号','工程项目','承建单位','备注']]
data_tmp1.rename(index=str, columns={"参建单位": "单位名称"},inplace=True)
data_tmp1['参与类型']='参建单位'
data_tmp2.rename(index=str, columns={"承建单位": "单位名称"},inplace=True)
data_tmp2['参与类型']='承建单位'
result=pd.concat([data_tmp1,data_tmp2])
result=result.copy()
result=result.reset_index()
result2=result.drop('单位名称', axis=1).join(result['单位名称'].str.split('、',expand=True).stack().reset_index(level=1, drop=True).rename('单位名称'))
result2=result2.drop(['发文文号','备注','index'],axis=1)
result2.drop_duplicates(['年度','工程项目','参与类型','单位名称'],inplace=True)
result2.to_excel(r'E:\xxxx\鲁班奖整.xlsx',sheet_name='sheet1')
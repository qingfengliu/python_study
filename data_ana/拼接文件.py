# coding=utf8
base_url=r'http://tz.ltd.cn:8888/default/input/projectBasicInfo/file_download1.jsp?' \
         r'attachPath=D:\Primeton\Platform\apache-tomcat-5.5.20\webapps\default\upload\176d175e61354ec4bff262254318f3d3.zip' \
         r'&attachType=zip&attachName=%s.zip'
tmps=[]
with open(r'D:\项目名.txt',encoding='utf8') as f:
    lines=f.readlines()
    for line in lines:
        line=line.strip()+'_合法合规性附件'
        tmps.append(base_url %line+'\n')
print(tmps)
with open(r'D:\连接名.txt','w',encoding='utf8') as f:
    f.writelines(tmps)

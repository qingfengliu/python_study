import xlwt

zidian={'从不':0,'偶尔':2,'几乎每晚':4,'每晚':5}
def write_line(worksheet,dates,linenum):
    for j,i in enumerate(dates):
        worksheet.write(linenum,j,i)

# 创建一个workbook 设置编码
workbook = xlwt.Workbook(encoding = 'utf-8')
# 创建一个worksheet
worksheet = workbook.add_sheet('My Worksheet')
write_line(worksheet,['打鼾程度','结果'],0)
linenum=0
for i in range(0,24):
    linenum+=1
    write_line(worksheet,[zidian['从不'],1],linenum)

for i in range(0,1355):
    linenum+=1
    write_line(worksheet,[zidian['从不'],0],linenum)

for i in range(0,35):
    linenum+=1
    write_line(worksheet,[zidian['偶尔'],1],linenum)

for i in range(0,603):
    linenum+=1
    write_line(worksheet,[zidian['偶尔'],0],linenum)

for i in range(0,21):
    linenum+=1
    write_line(worksheet,[zidian['几乎每晚'],1],linenum)

for i in range(0,192):
    linenum+=1
    write_line(worksheet,[zidian['几乎每晚'],0],linenum)

for i in range(0,30):
    linenum+=1
    write_line(worksheet,[zidian['每晚'],1],linenum)

for i in range(0,224):
    linenum+=1
    write_line(worksheet,[zidian['每晚'],0],linenum)

workbook.save(r'D:\学习\数据\打鼾展开值.xls')
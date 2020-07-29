import statsmodels.api as sm
import pandas as pd
datas=pd.read_excel(r'D:\学习\数据\打鼾展开值.xlsx')
x=pd.DataFrame(datas['打鼾程度'])
y=datas['结果']
X=sm.add_constant(x)
est=sm.Probit(y,X)
est=est.fit()
print(est.summary())
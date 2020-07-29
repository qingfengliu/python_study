from sklearn import  linear_model
import pandas as pd
from scipy.stats import linregress
import statsmodels.api as sm

datas=pd.read_excel(r'D:\学习\数据\打鼾展开值.xlsx')
x=pd.DataFrame(datas['打鼾程度'])
y=datas['结果']
clf = linear_model.LinearRegression()
clf.fit(x, y)
a, b = clf.coef_, clf.intercept_
print('回归系数: ', a,'\n截距:',b)
slope, intercept, r_value, p_value, std_err =linregress(datas['打鼾程度'],datas['结果'])
print('回归系数: ', slope,'\n截距:',b)

X=sm.add_constant(x)
est=sm.OLS(y,X)
est=est.fit()
print(est.summary())
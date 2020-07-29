from sklearn.linear_model import LogisticRegression
import pandas as pd
import statsmodels.api as sm

datas=pd.read_excel(r'D:\学习\数据\母鲎及其追随者案例.xlsx')
# x=pd.DataFrame(datas['甲壳宽度'])
# y=datas['追随者数']
# L =LogisticRegression(solver = 'lbfgs',multi_class = 'ovr')
# L.fit(x,y)
# a, b = L.coef_, L.intercept_
# print('回归系数: ', a,'\n截距:',b)

X=sm.add_constant(x)
est=sm.Logit(y,X)
est=est.fit()
print(est.summary())
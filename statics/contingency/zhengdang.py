import numpy as np
from scipy.stats import chi2_contingency

d = np.array([[762, 327, 468], [484, 239, 477]])
print(chi2_contingency(d))

#chi2表示卡方检验值，p表示P值，dof表示自由度，ex表示每个格子P（AB）应该的概率



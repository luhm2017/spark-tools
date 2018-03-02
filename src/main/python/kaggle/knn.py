import numpy as np
from sklearn import neighbors

#取得 knn 分类器
knn = neighbors.KNeighborsClassifier()
data = np.array([[3,104],[2,100],[1,81],[101,10],[99,5],[98,2]])
labels = np.array([1,1,1,2,2,2])
#导入数据进行训练
knn.fit(data,labels)
#knn.predict([18,90])
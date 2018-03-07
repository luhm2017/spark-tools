from sklearn import datasets,linear_model
from sklearn.model_selection import cross_validate

diabetes = datasets.load_diabetes()
X = diabetes.data[:150]
y = diabetes.target[:150]
lasso = linear_model.Lasso()

cv_results = cross_validate(lasso, X, y, return_train_score=False)
sorted(cv_results.keys())
cv_results['test_score']

scores = cross_validate(lasso, X, y,scoring=('r2', 'neg_mean_squared_error'))
print(scores['test_neg_mean_squared_error'])
print(scores['train_r2'])


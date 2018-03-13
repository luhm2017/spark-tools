# Visualization
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
# Common Model Helpers
from sklearn import model_selection
# Common Model Algorithms
from sklearn import svm, tree, linear_model, neighbors, naive_bayes, ensemble, discriminant_analysis, gaussian_process
from sklearn.preprocessing import LabelEncoder

sns.set_style('white')

# load data
data_raw = pd.read_csv("D:/workspace/spark-tools/src/main/data/titanic/train_titanic.csv")
data_val = pd.read_csv("D:/workspace/spark-tools/src/main/data/titanic/test_titanic.csv")
data1 = data_raw.copy(deep=True)
m = data1["Embarked"].mode()
name = data1['Name'].str.split(", ", expand=True)[1].str.split(".", expand=True)[0]
data_cleaner = [data1,data_val]

# data prospecting
# print(data_raw.sample(10))
print("####原始数据####")
# data_raw.info()
# data_val.info()
# print(data_raw.describe(include="all"))

# data processing
# 同步处理训练数据和测试数据
for dataset in data_cleaner:
    # 中位数 填充缺失值
    dataset["Age"].fillna(dataset["Age"].median(),inplace=True)
    # 众位数 填充缺失值
    dataset["Embarked"].fillna(dataset["Embarked"].mode()[0],inplace=True)
    # Fare 中位数填充
    dataset["Fare"].fillna(dataset["Fare"].median(),inplace=True)

# 删除无业务意义以及缺失率过高的字段
drop_columns = ["PassengerId","Ticket","Cabin"]
data_val.drop(drop_columns,axis=1,inplace=True)

# feature engineering 特征工程处理
for dataset in data_cleaner:
    # Discrete variables
    dataset["FamilySize"] = dataset["SibSp"] + dataset["Parch"] + 1
    dataset["IsAlone"] = 1
    # 根据FamilySize判断是否alone
    dataset["IsAlone"].loc[dataset["FamilySize"] > 1] = 0 # 索引到对应的数据，然后更新
    #  根据姓名获取对应称呼，Mr Mrs Miss等
    dataset["Title"] = dataset["Name"].str.split(", ",expand=True)[1].str.split(".",expand=True)[0]
    # 连续型变量分箱处理，qcut 为等频分箱 ，cut为等宽分箱
    dataset['FareBin'] = pd.qcut(dataset['Fare'], 4)
    dataset['AgeBin'] = pd.cut(dataset['Age'].astype(int), 5)

# title 对名称字段处理
stat_min = 10
# 设定少数类别的阀值，返回结果为每个类别，可以预先勘探title类别的分布情况
title_names = (data1['Title'].value_counts() < stat_min)
data1['Title'] = data1['Title'].apply(lambda x: 'Misc' if title_names.loc[x] == True else x)

# preview data again
# data1.info()
# data_val.info()
# print(data1.sample(10))

# 将类别变量转换成哑变量 convert categorical data to dummy variables
# pclass_dummies_titanic  = pd.get_dummies(data1['Pclass'])
# # pclass_dummies_titanic.drop(['3'], axis=1, inplace=True)
# data1 = data1.join(pclass_dummies_titanic)

# 遍历处理，将类别变量code处理
label = LabelEncoder()
for dataset in data_cleaner:
    # sex 类别值为2
    dataset['Sex_Code'] = label.fit_transform(dataset['Sex'])
    dataset["Embarked_Code"] = label.fit_transform(dataset["Embarked"])
    dataset["Title_Code"] = label.fit_transform(dataset["Title"])
    # 连续型变量code化处理
    dataset["FareBin_Code"] = label.fit_transform(dataset["FareBin"])
    dataset["AgeBin_Code"] = label.fit_transform(dataset["AgeBin"])

# preview data again
# print(data1.info())
# print(data1.sample(10))
Target = ["Survived"]

# define x variables for original features
data1_x = ['Sex','Pclass', 'Embarked', 'Title','SibSp', 'Parch', 'Age', 'Fare', 'FamilySize', 'IsAlone']
# 将类别变量code化处理
data1_x_calc = ['Sex_Code','Pclass', 'Embarked_Code', 'Title_Code','SibSp', 'Parch', 'Age', 'Fare']
data1_xy =  Target + data1_x
# define x variables for original w/bin features to remove continuous variables
data1_x_bin = ['Sex_Code','Pclass', 'Embarked_Code', 'Title_Code', 'FamilySize', 'AgeBin_Code', 'FareBin_Code']
data1_xy_bin = Target + data1_x_bin

# define x and y variables for dummy features original
data1_dummy = pd.get_dummies(data1[data1_x])
data1_x_dummy = data1_dummy.columns.tolist()
data1_xy_dummy = Target + data1_x_dummy
print('Dummy X Y: ', data1_xy_dummy, '\n')

# ############################################单变量分析处理
# 遍历分析 离散变量与y的相关性
for x in data1_x:
    if data1[x].dtype != 'float64':
        print('Survival Correlation by:', x)
        print(data1[[x, Target[0]]].groupby(x, as_index=False).mean())
        print('-'*10, '\n')

# 箱线图 graph distribution of quantitative data
# plt常见画图方法，箱线图boxplot，直方图hist
plt.figure(figsize=[16,12]) # 尺寸大小单位为英寸

plt.subplot(231) # 表示子图2*3，后面编号表示序列
plt.boxplot(x=data1['Fare'], showmeans = True, meanline = True)
plt.title('Fare Boxplot')
plt.ylabel('Fare ($)')

plt.subplot(232)
plt.boxplot(data1['Age'], showmeans = True, meanline = True)
plt.title('Age Boxplot')
plt.ylabel('Age (Years)')

plt.subplot(233)
plt.boxplot(data1['FamilySize'], showmeans = True, meanline = True)
plt.title('Family Size Boxplot')
plt.ylabel('Family Size (#)')

# 直方图分析 fare分布
plt.subplot(234)
plt.hist(x = [data1[data1['Survived']==1]['Fare'], data1[data1['Survived']==0]['Fare']],
         stacked=True, color = ['g','r'],label = ['Survived','Dead'])
plt.title('Fare Histogram by Survival')
plt.xlabel('Fare ($)')
plt.ylabel('# of Passengers')
plt.legend()

# 直方图 age分布
plt.subplot(235)
plt.hist(x = [data1[data1['Survived']==1]['Age'], data1[data1['Survived']==0]['Age']],
         stacked=True, color = ['g','r'],label = ['Survived','Dead'])
plt.title('Age Histogram by Survival')
plt.xlabel('Age (Years)')
plt.ylabel('# of Passengers')
plt.legend()

# 直方图 familysize 分布
plt.subplot(236)
plt.hist(x = [data1[data1['Survived']==1]['FamilySize'], data1[data1['Survived']==0]['FamilySize']],
         stacked=True, color = ['g','r'],label = ['Survived','Dead'])
plt.title('Family Size Histogram by Survival')
plt.xlabel('Family Size (#)')
plt.ylabel('# of Passengers')
plt.legend()
# plt.show()

# graph individual features by survival
fig, saxis = plt.subplots(2, 3,figsize=(16,12))
# 使用seaborn 矩阵图
sns.barplot(x = 'Embarked', y = 'Survived', data=data1, ax = saxis[0,0])
sns.barplot(x = 'Pclass', y = 'Survived', order=[1,2,3], data=data1, ax = saxis[0,1])
sns.barplot(x = 'IsAlone', y = 'Survived', order=[1,0], data=data1, ax = saxis[0,2])

sns.pointplot(x = 'FareBin', y = 'Survived',  data=data1, ax = saxis[1,0])
sns.pointplot(x = 'AgeBin', y = 'Survived',  data=data1, ax = saxis[1,1])
sns.pointplot(x = 'FamilySize', y = 'Survived', data=data1, ax = saxis[1,2])
# plt.show()

########################################
# 组合特征变量的分析
# graph distribution of qualitative data: Pclass
# we know class mattered in survival, now let's compare class and a 2nd feature
fig, (axis1,axis2,axis3) = plt.subplots(1,3,figsize=(14,12))

sns.boxplot(x = 'Pclass', y = 'Fare', hue = 'Survived', data = data1, ax = axis1)
axis1.set_title('Pclass vs Fare Survival Comparison')

sns.violinplot(x = 'Pclass', y = 'Age', hue = 'Survived', data = data1, split = True, ax = axis2)
axis2.set_title('Pclass vs Age Survival Comparison')

sns.boxplot(x = 'Pclass', y ='FamilySize', hue = 'Survived', data = data1, ax = axis3)
axis3.set_title('Pclass vs Family Size Survival Comparison')

# graph distribution of qualitative data: Sex
# we know sex mattered in survival, now let's compare sex and a 2nd feature
fig, qaxis = plt.subplots(1,3,figsize=(14,12))

sns.barplot(x = 'Sex', y = 'Survived', hue = 'Embarked', data=data1, ax = qaxis[0])
axis1.set_title('Sex vs Embarked Survival Comparison')

sns.barplot(x = 'Sex', y = 'Survived', hue = 'Pclass', data=data1, ax  = qaxis[1])
axis1.set_title('Sex vs Pclass Survival Comparison')

sns.barplot(x = 'Sex', y = 'Survived', hue = 'IsAlone', data=data1, ax  = qaxis[2])
axis1.set_title('Sex vs IsAlone Survival Comparison')

# 针对年龄做划分，分析y的比率
# plot distributions of age of passengers who survived or did not survive
# a = sns.FacetGrid( data1, hue = 'Survived', aspect=4 )
# a.map(sns.kdeplot, 'Age', shade= True )
# a.set(xlim=(0 , data1['Age'].max()))
# a.add_legend()

# correlation heatmap of dataset
def correlation_heatmap(df):
    _ , ax = plt.subplots(figsize =(14, 12))
    colormap = sns.diverging_palette(220, 10, as_cmap = True)

    _ = sns.heatmap(
        df.corr(),
        cmap = colormap,
        square=True,
        cbar_kws={'shrink':.9 },
        ax=ax,
        annot=True,
        linewidths=0.1,vmax=1.0, linecolor='white',
        annot_kws={'fontsize':12 }
    )

    plt.title('Pearson Correlation of Features', y=1.05, size=15)
    # plt.show()

# correlation_heatmap(data1)

# split train and test data
# split train and test data with function defaults
train1_x, test1_x, train1_y, test1_y = model_selection.train_test_split(data1[data1_x_calc], data1[Target], random_state = 0)
train1_x_bin, test1_x_bin, train1_y_bin, test1_y_bin = model_selection.train_test_split(data1[data1_x_bin], data1[Target] , random_state = 0)
train1_x_dummy, test1_x_dummy, train1_y_dummy, test1_y_dummy = model_selection.train_test_split(data1_dummy[data1_x_dummy], data1[Target], random_state = 0)

# machine learning algorithm
MLA = [
    # ensemble methods
    ensemble.AdaBoostClassifier(),
    ensemble.BaggingClassifier(),
    ensemble.ExtraTreesClassifier(),
    ensemble.GradientBoostingClassifier(),
    ensemble.RandomForestClassifier(),

    # Gaussian processes
    gaussian_process.GaussianProcessClassifier(),

    # GLM
    linear_model.LogisticRegressionCV(),
    linear_model.PassiveAggressiveClassifier(),
    linear_model.RidgeClassifierCV(),
    linear_model.SGDClassifier(),
    linear_model.Perceptron(),

    # Navies Bayes
    naive_bayes.BernoulliNB(),
    naive_bayes.GaussianNB(),

    # Nearest Neighbor
    neighbors.KNeighborsClassifier(),

    # SVM
    svm.SVC(probability=True),
    svm.NuSVC(probability=True),
    svm.LinearSVC(),

    # Trees
    tree.DecisionTreeClassifier(),
    tree.ExtraTreeClassifier(),

    # Discriminant Analysis
    discriminant_analysis.LinearDiscriminantAnalysis(),
    discriminant_analysis.QuadraticDiscriminantAnalysis()
]

# split dataset in cross-validation ，交叉切分数据集
# random_state = 0 表示每次随机数都不同
cv_split = model_selection.ShuffleSplit(n_splits=10,test_size=0.3,train_size=0.6,random_state=0)
# list
MLA_columns = ['MLA Name','MLA Parameters','MLA Train Accuracy Mean','MLA Test Accuracy Mean','MLA Test Accuracy 3*STD' ,'MLA Time']
MLA_compare = pd.DataFrame(columns= MLA_columns)
# predictions
MLA_predict = data1[Target]

# index through MLA and save perfomance to table
row_index = 0
for alg in MLA:
    # set name and parameters
    # 类名
    MLA_name = alg.__class__.__name__
    MLA_compare.loc[row_index,'MLA Name'] = MLA_name
    MLA_compare.loc[row_index,'MLA Parameters'] = str(alg.get_params())
    # score model with cross validation
    # 模型评分 ？？
    cv_results = model_selection.cross_validate(alg, data1[data1_x_bin], data1[Target], cv  = cv_split)

    MLA_compare.loc[row_index, 'MLA Time'] = cv_results['fit_time'].mean()
    MLA_compare.loc[row_index, 'MLA Train Accuracy Mean'] = cv_results['train_score'].mean()
    MLA_compare.loc[row_index, 'MLA Test Accuracy Mean'] = cv_results['test_score'].mean()
    MLA_compare.loc[row_index, 'MLA Test Accuracy 3*STD'] = cv_results['test_score'].std()*3

    # save MLA predictions - see section 6 for usage
    alg.fit(data1[data1_x_bin], data1[Target])
    MLA_predict[MLA_name] = alg.predict(data1[data1_x_bin])

    row_index+=1

MLA_compare.sort_values(by = ['MLA Test Accuracy Mean'], ascending = False, inplace = True)
print(MLA_compare)
#MLA_predict

# split
X_train,X_test,Y_train,Y_test = model_selection.train_test_split(data1[data1_x_bin], data1[Target],test_size=0.1,random_state=0)

# 使用随机森林
random_forest = ensemble.RandomForestClassifier(n_estimators=100)
random_forest.fit(X_train, Y_train)
Y_pred = random_forest.predict(X_test)
rf_score = random_forest.score(X_test, Y_test)
print(rf_score)

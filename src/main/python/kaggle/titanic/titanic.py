# load data
import pandas as pd
import numpy as np
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')

# machine learning
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC, LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.model_selection import train_test_split

# get titanic & test csv files as a DataFrame
titanic_df = pd.read_csv("D:/workspace/spark-tools/src/main/data/titanic/train_titanic.csv")
test_df = pd.read_csv("D:/workspace/spark-tools/src/main/data/titanic/test_titanic.csv")
# preview the data
# print(titanic_df.head())
# titanic_df.sample(5)
# titanic_df.info()
# test_df.info()
# Data preprocessing
titanic_df = titanic_df.drop(["PassengerId","Name","Ticket"],axis=1)
test_df  = test_df.drop(['Name','Ticket'], axis=1)


# Embarked 缺失值处理，取众数填充
titanic_df["Embarked"] = titanic_df["Embarked"].fillna("S")
# plot 绘制变量关系图，表示两变量的分布情况
sns.factorplot('Embarked','Survived', data=titanic_df,size=4,aspect=3)
# 表示绘制3张子图
fig, (axis1,axis2,axis3) = plt.subplots(1,3,figsize=(15,5))

# sns.factorplot('Embarked',data=titanic_df,kind='count',order=['S','C','Q'],ax=axis1)
# sns.factorplot('Survived',hue="Embarked",data=titanic_df,kind='count',order=[1,0],ax=axis2)
# 统计 Embarked 分布
sns.countplot(x='Embarked', data=titanic_df, ax=axis1)
# 统计好坏样本中，Embarked的分布情况
sns.countplot(x='Survived', hue="Embarked", data=titanic_df, order=[1,0], ax=axis2)
# group by embarked, and get the mean for survived passengers for each value in Embarked
embark_perc = titanic_df[["Embarked", "Survived"]].groupby(['Embarked'],as_index=False).mean()
sns.barplot(x='Embarked', y='Survived', data=embark_perc,order=['S','C','Q'],ax=axis3)

# Either to consider Embarked column in predictions,
# and remove "S" dummy variable,
# and leave "C" & "Q", since they seem to have a good rate for Survival.

# OR, don't create dummy variables for Embarked column, just drop it,
# because logically, Embarked doesn't seem to be useful in prediction.
# titanic_df Embarked 哑变量转换
embark_dummies_titanic  = pd.get_dummies(titanic_df['Embarked'])
embark_dummies_titanic.drop(['S'], axis=1, inplace=True)

embark_dummies_test  = pd.get_dummies(test_df['Embarked'])
embark_dummies_test.drop(['S'], axis=1, inplace=True)

titanic_df = titanic_df.join(embark_dummies_titanic)
test_df    = test_df.join(embark_dummies_test)

titanic_df.drop(['Embarked'], axis=1,inplace=True)
test_df.drop(['Embarked'], axis=1,inplace=True)

# Fare 测试样本中缺失一条记录，使用中值填补
test_df["Fare"].fillna(test_df["Fare"].median(),inplace=True)
# convert from float to int
titanic_df["Fare"] = titanic_df["Fare"].astype(int)
test_df["Fare"] = titanic_df["Fare"].astype(int)
# get fare for survived & didn't survive passengers
fare_no_survived = titanic_df["Fare"][titanic_df["Survived"] == 0]
fare_survived = titanic_df["Fare"][titanic_df["Survived"] == 1]
# get average and std for fare of survived/not survived passengers
average_fare = DataFrame([fare_no_survived.mean(),fare_survived.mean()])
std_fare = DataFrame([fare_no_survived.std(),fare_survived.std()])

# plot
# 绘制 Fare 的直方分布图
# titanic_df['Fare'].plot(kind='hist', figsize=(15,3),bins=100, xlim=(0,50))
# average_fare.index.names = std_fare.index.names = ["Survived"]
# average_fare.plot(yerr=std_fare,kind='bar',legend=False)

# age
fig, (axis1,axis2) = plt.subplots(1,2,figsize=(15,4))
axis1.set_title('Original Age values - Titanic')
axis2.set_title('New Age values - Titanic')

# get average, std, and number of NaN values in titanic_df
average_age_titanic   = titanic_df["Age"].mean()
std_age_titanic       = titanic_df["Age"].std()
count_nan_age_titanic = titanic_df["Age"].isnull().sum()

# get average, std, and number of NaN values in test_df
average_age_test   = test_df["Age"].mean()
std_age_test       = test_df["Age"].std()
count_nan_age_test = test_df["Age"].isnull().sum()

# generate random numbers between (mean - std) & (mean + std)
# 以均值的一个标准差为区间数，用以填补age的缺失值
rand_1 = np.random.randint(average_age_titanic-std_age_titanic,average_age_titanic+std_age_titanic,count_nan_age_titanic)
rand_2 = np.random.randint(average_age_test-std_age_test,average_age_test+std_age_test,count_nan_age_test)

# plot original Age values
# NOTE: drop all null values, and convert to int
titanic_df["Age"].dropna().astype(int).hist(bins=70,ax=axis1)
# fill NaN values in Age column with random values generated
titanic_df["Age"][np.isnan(titanic_df["Age"])] = rand_1
test_df["Age"][np.isnan(test_df["Age"])] = rand_2
# convert from float to int
titanic_df['Age'] = titanic_df['Age'].astype(int)
test_df['Age']    = test_df['Age'].astype(int)
# 绘制填充后age的直方分布图， 主要比对做填充处理后的分布图
titanic_df["Age"].hist(bins=70,ax=axis2)

# peaks for survived/not survived passengers by their age
# 根据age 绘制分布01分布情况，不同年龄段幸存率不同
# facet = sns.FacetGrid(titanic_df, hue="Survived",aspect=4)
# facet.map(sns.kdeplot,'Age',shade= True)
# facet.set(xlim=(0, titanic_df['Age'].max()))
# facet.add_legend()
# fig, axis1 = plt.subplots(1,1,figsize=(18,4))
# average_age = titanic_df[["Age", "Survived"]].groupby(['Age'],as_index=False).mean()
# sns.barplot(x='Age', y='Survived', data=average_age)

# Cabin
# It has a lot of NaN values, so it won't cause a remarkable impact on prediction
titanic_df.drop("Cabin",axis=1,inplace=True)
test_df.drop("Cabin",axis=1,inplace=True)


# Family

# Instead of having two columns Parch & SibSp,
# we can have only one column represent if the passenger had any family member aboard or not,
# Meaning, if having any family member(whether parent, brother, ...etc) will increase chances of Survival or not.
# 根据是否有家属判断幸存率 ，进一步数据可以对家属人数进行分段分析
titanic_df['Family'] =  titanic_df["Parch"] + titanic_df["SibSp"]
titanic_df['Family'].loc[titanic_df['Family'] > 0] = 1
titanic_df['Family'].loc[titanic_df['Family'] == 0] = 0

test_df['Family'] =  test_df["Parch"] + test_df["SibSp"]
test_df['Family'].loc[test_df['Family'] > 0] = 1
test_df['Family'].loc[test_df['Family'] == 0] = 0

# drop Parch & SibSp
titanic_df = titanic_df.drop(['SibSp','Parch'], axis=1)
test_df    = test_df.drop(['SibSp','Parch'], axis=1)

# plot
fig, (axis1,axis2) = plt.subplots(1,2,sharex=True,figsize=(10,5))
# sns.factorplot('Family',data=titanic_df,kind='count',ax=axis1)
sns.countplot(x='Family', data=titanic_df, order=[1,0], ax=axis1)
# average of survived for those who had/didn't have any family member
family_perc = titanic_df[["Family", "Survived"]].groupby(['Family'],as_index=False).mean()
sns.barplot(x='Family', y='Survived', data=family_perc, order=[1,0], ax=axis2)

# 更改别名
axis1.set_xticklabels(["With Family","Alone"], rotation=0)

# Sex
# 不同性别的乘客，幸存率不同。将乘客分成 males、females以及child
def get_person(passenger):
    age,sex = passenger
    return "child" if age < 16 else sex
# 依据sex age来判断
titanic_df["Person"] = titanic_df[["Age","Sex"]].apply(get_person,axis=1)
test_df['Person']    = test_df[['Age','Sex']].apply(get_person,axis=1)

# No need to use Sex column since we created Person column
titanic_df.drop(['Sex'],axis=1,inplace=True)
test_df.drop(['Sex'],axis=1,inplace=True)

# create dummy variables for Person column, & drop Male as it has the lowest average of survived passengers
# 单变量分析
person_dummies_titanic  = pd.get_dummies(titanic_df['Person'])
person_dummies_titanic.columns = ['Child','Female','Male']
person_dummies_titanic.drop(['Male'], axis=1, inplace=True)

person_dummies_test  = pd.get_dummies(test_df['Person'])
person_dummies_test.columns = ['Child','Female','Male']
person_dummies_test.drop(['Male'], axis=1, inplace=True)

titanic_df = titanic_df.join(person_dummies_titanic)
test_df    = test_df.join(person_dummies_test)

fig, (axis1,axis2) = plt.subplots(1,2,figsize=(10,5))

# sns.factorplot('Person',data=titanic_df,kind='count',ax=axis1)
sns.countplot(x='Person', data=titanic_df, ax=axis1)

# average of survived for each Person(male, female, or child)
person_perc = titanic_df[["Person", "Survived"]].groupby(['Person'],as_index=False).mean()
sns.barplot(x='Person', y='Survived', data=person_perc, ax=axis2, order=['male','female','child'])

titanic_df.drop(['Person'],axis=1,inplace=True)
test_df.drop(['Person'],axis=1,inplace=True)

# Pclass 单变量分析方式
# 统计每个等级的占比
fig, (axis1,axis2) = plt.subplots(1,2,figsize=(10,5))
sns.countplot(x='Pclass', data=titanic_df, ax=axis1)
pclass_perc = titanic_df[["Pclass", "Survived"]].groupby(['Pclass'],as_index=False).mean()
sns.barplot(x='Pclass', y='Survived', data=pclass_perc, ax=axis2, order=[1,2,3])

# 哑变量处理
pclass_dummies_titanic  = pd.get_dummies(titanic_df['Pclass'])
pclass_dummies_titanic.columns = ['Class_1','Class_2','Class_3']
pclass_dummies_titanic.drop(['Class_3'], axis=1, inplace=True)

pclass_dummies_test  = pd.get_dummies(test_df['Pclass'])
pclass_dummies_test.columns = ['Class_1','Class_2','Class_3']
pclass_dummies_test.drop(['Class_3'], axis=1, inplace=True)

titanic_df.drop(['Pclass'],axis=1,inplace=True)
test_df.drop(['Pclass'],axis=1,inplace=True)

titanic_df = titanic_df.join(pclass_dummies_titanic)
test_df    = test_df.join(pclass_dummies_test)

# split data into train and test
X_train_original = titanic_df.drop("Survived",axis=1)
Y_train_original = titanic_df["Survived"]
X_test_original  = test_df.drop("PassengerId",axis=1).copy()

# split
X_train,X_test,Y_train,Y_test = train_test_split(X_train_original,Y_train_original,test_size=0.1,random_state=3)

# logistic regression
logreg = LogisticRegression()
logreg.fit(X_train,Y_train)
Y_pred = logreg.predict(X_test_original)
lr_score = logreg.score(X_test, Y_test)
print(lr_score)

# Random Forests
random_forest = RandomForestClassifier(n_estimators=100)
random_forest.fit(X_train, Y_train)
Y_pred = random_forest.predict(X_test_original)
rf_score = random_forest.score(X_test, Y_test)
print(rf_score)

# get Correlation Coefficient for each feature using Logistic Regression
# coeff_df = DataFrame(titanic_df.columns.delete(0))
# coeff_df.columns = ['Features']
# coeff_df["Coefficient Estimate"] = pd.Series(logreg.coef_[0])
#
# # preview
# coeff_df
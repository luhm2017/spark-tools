//titanic_train for
kaggle_titanic_train_data

##Data Dictionary
Variable	Definition	Key
survival	Survival	0 = No, 1 = Yes
pclass	Ticket class	1 = 1st, 2 = 2nd, 3 = 3rd
sex	Sex
Age	Age in years
sibsp	# of siblings / spouses aboard the Titanic
parch	# of parents / children aboard the Titanic
ticket	Ticket number
fare	Passenger fare
cabin	Cabin number
embarked	Port of Embarkation	C = Cherbourg, Q = Queenstown, S = Southampton


##Variable Notes
pclass: A proxy for socio-economic status (SES)
1st = Upper
2nd = Middle
3rd = Lower

age: Age is fractional if less than 1. If the age is estimated, is it in the form of xx.5
sibsp: The dataset defines family relations in this way...
Sibling = brother, sister, stepbrother, stepsister
Spouse = husband, wife (mistresses and fiancés were ignored)
parch: The dataset defines family relations in this way...
Parent = mother, father
Child = daughter, son, stepdaughter, stepson
Some children travelled only with a nanny, therefore parch=0 for them.


##数据分析
PassengerId => 乘客ID
Pclass => 乘客等级(1/2/3等舱位) --onehot编码处理
Name => 乘客姓名
Sex => 性别
Age => 年龄
SibSp => 堂兄弟/妹个数
Parch => 父母与小孩个数
Ticket => 船票信息
Fare => 票价
Cabin => 客舱
Embarked => 登船港口

#数据处理：
1、使用随机森林或者线性回归来预测年龄
2、统计各姓名类别（Miss\Mrs\Mr）的平均年龄
3、使用众数或者均数替代缺失值
4、对标称型变量使用oneHot编码处理
5、特征合并：SibSp+Parch 表示家庭大小，包括衍生变量是否为一个人
6、所有字段单变量分析，该变量值应对正负样本的比例，即WOE、IV值分析
7、对船票进行分箱、分段处理，如何分段？
8、同样，对age也进行分段处理
9、衍生变量加工：Age*Pclass等，增加年龄与舱位等级的权重等
10、基于衍生的变量，PCA进行维归约以及特征相关性分析
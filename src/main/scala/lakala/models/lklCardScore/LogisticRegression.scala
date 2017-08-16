package lakala.models.lklCardScore

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Administrator on 2017/2/13.
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark Logistic Regression")
    val sc = new SparkContext(sparkConf)

    if(args.length!=3){
      println("请输入参数：垃圾邮件数据集path、正常邮件数据集path、模型运行时间")
      System.exit(0)
    }
    //path
    val path = args(0)
    //垃圾邮件数据
    val spam = sc.textFile(args(0))
    //正常邮件数据
    val normal = sc.textFile(args(1))
    //模型运行时间
    val date = args(2)

    //邮件都被切分为单词，返回vector
    val spamFeatures = spam.map(email =>documentToVector((email.split(" "))))
    val normalFeatures = normal.map(email => documentToVector(email.split(" ")))

    //创建LabeledPoint数据集分别存放垃圾邮件(spam)和正常邮件(ham)的例子
    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val sampleData = positiveExamples.union(negativeExamples)
    sampleData.cache() // 逻辑回归是迭代算法，所以缓存训练数据的RDD

    //获取不同的训练数据集
    // Split data into training (60%) and test (40%)
    val Array(trainingData, testData) = sampleData.randomSplit(Array(0.6, 0.4), seed = 11L)

    //使用SGD算法运行逻辑回归
    val lrLearner = new LogisticRegressionWithSGD()
    val model = lrLearner.run(trainingData)

    // Clear the prediction threshold so the model will return probabilities
    model.clearThreshold

    // Compute raw scores on the testData set
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    //--===============================================================================
    // Instantiate metrics object
    //使用BinaryClassificationMetrics评估
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // Precision by threshold
    //列出所有阈值的p,r,f值
    val precision = metrics.precisionByThreshold
    precision.map({case (t, p) =>
      "Threshold: "+t+"Precision:"+p
    }).saveAsTextFile(s"hdfs://ns1/tmp/$date/precision")
    /*precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }*/

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.map({case (t, r) =>
      "Threshold: "+t+"Recall:"+r
    }).saveAsTextFile(s"hdfs://ns1/tmp/$date/recall")
    /*recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }*/

    // Precision-Recall Curve
    val prc = metrics.pr
    prc.map({case (t, r) =>
      t+" :"+r
    }).saveAsTextFile(s"hdfs://ns1/tmp/$date/prc")
    /*prc.foreach{
      case(x,y) =>
        println(s"Curve: $x,$y")
    }*/

    //the beta factor in F-Measure computation.
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    fScore.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC，精度，召回曲线下的面积
    val auPRC = metrics.areaUnderPR
    sc.makeRDD(Seq("Area under precision-recall curve = " +auPRC)).saveAsTextFile(s"hdfs://ns1/tmp/$date/auPRC")
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc
    sc.makeRDD(Seq("ROC Curve = " + roc)).saveAsTextFile(s"hdfs://ns1/tmp/$date/roc")
    println("ROC Curve = " + roc)

    // AUC
    val auROC = metrics.areaUnderROC
    sc.makeRDD(Seq("Area under ROC = " + +auROC)).saveAsTextFile(s"hdfs://ns1/tmp/$date/auROC")
    println("Area under ROC = " + auROC)
    sc.stop()

  }

  def loadDataForAdult(sc : SparkContext,path:String): LabeledPoint ={
    val adult = sc.textFile(path)
    return null
  }

  //将document转换成vectors
  def documentToVector(document:Array[_]): Vector ={
    val termFrequencies = mutable.HashMap.empty[Int,Double]
    //遍历文档，统计每个单词累计值
    document.foreach{
      term =>
        //获取单词的哈希值
        val i = indexOf(term)
        //统计单词的频数
        val tmp = termFrequencies.getOrElse(i, 0.0) + 1.0
        termFrequencies.put(i, tmp)
    }
    //稀疏矩阵
    Vectors.sparse(1 << 20, termFrequencies.toSeq)
  }

  /**
    * Returns the index of the input term.
    * term.## 单词对应的hash值
    * mod 1<<20 维数
    */
  def indexOf(term: Any): Int = nonNegativeMod(term.##, 1 << 20)

  //单词hash值取模
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}

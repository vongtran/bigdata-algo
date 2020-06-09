import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}
import au.com.bytecode.opencsv.CSVParser
import scala.math._
import spark.implicits._
import scala.collection.mutable.ListBuffer

object Bootstrap {
    /**
      * Bootstrap class loads data from a csv file and calculates mean and standard deviation.
      * This class also do sampling and calculates the averages of mean and standard deviation during sampling
      * @param args[0] csv file
      * @param args[1] group column number
      * @param args[2] feature column number
      * @param args[3] sampling time number
      * @param args[4] percentage sampling number
      */
    def main(args: Array[String]) {
     
        // initilize configuration
        val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
        // val sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
        
        /**
          * calMiuSigma function iterates through groups and calcuate miu (mean) and sigma (standard deviation)  
          * values for each group
          * @param arr:  collection of feature grouped by group name
          * @return collection of group with miu and sigma value
          */
        def calMiuSigma(arr : Iterable[(String, Double, Double)]) : Iterable[(String, Double, Double)] = {
            val arrGroup = arr.groupBy(x => x._1)
            val arrGroupMap = arrGroup.map(x => (x._1, (x._2.count(_=>true), x._2.reduce((a,b)=>(a._1,a._2 + b._2, a._3 + b._3)))))
            val miuAndSigma = arrGroupMap.map(x => (x._1, x._2._2._2/x._2._1, sqrt((x._2._2._3/x._2._1) - pow(x._2._2._2/x._2._1,2))))
            miuAndSigma
        }

        //read csv file 
        // val csv = sc.textFile("/user/cloudera/input/mtcars.csv")
        val csvFile = args(0);
        val csv = sc.textFile(csvFile)
        val headerAndRows = csv.map(line => line.split(",").map(_.trim))
        val header = headerAndRows.first
        val rawData = headerAndRows.filter(_(0) != header(0))
        
        //create population
        val groupColumnNumber = args(1).toInt
        val featureColumnNumber = args(2).toInt
        val finedData = rawData.map(x => (x(groupColumnNumber),x(featureColumnNumber).toDouble, x(1).toDouble*x(1).toDouble))
        
        // calculate miu and sigma for the whole data
        val miusigma = calMiuSigma(finedData.collect())
        println("Mean and Standard Deviation for the whole data: ")
        miusigma.foreach(println)

        //sampling data
        val samplingTime = args(3).toInt
        val percentageSampling = args(4).toDouble
        val samplingData = finedData.sample(false, percentageSampling)
        
        var collectList = new ListBuffer[(String, Double, Double)]()
        var x = 0
        for(x <- 1 to samplingTime) {
            val samplingTmp = samplingData.sample(true, 1)
            val mnsTmp = calMiuSigma(samplingTmp.collect())
            collectList = collectList ++ mnsTmp
        }
        val collectListGroup = collectList.groupBy(x => x._1)
        val collectListGroupMap = collectListGroup.map(x => (x._1, (x._2.count(_=>true), x._2.reduce((a,b)=>(a._1,a._2 + b._2, a._3 + b._3)))))
        val aveMiuAndSigma = collectListGroupMap.map(x => (x._1, x._2._2._2/x._2._1, x._2._2._3/x._2._1))
        println("Average Mean and Standard Deviation for sampling data: ")
        aveMiuAndSigma.foreach(println)

        
    }
}
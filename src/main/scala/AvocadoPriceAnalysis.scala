import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.sql.Date

import org.apache.spark.sql.types.DateType

object AvocadoPriceAnalysis {

  def main(Args:Array[String]): Unit =
  {
    System.setProperty("hadoop.home.dir","C:/Users/Nitesh/IdeaProjects/")
    val conf=new SparkConf().setMaster("local").setAppName("AvocadoPriceAnalysis")
    val spark=SparkSession.builder().appName("AvocadoPriceAnalysis").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val input=spark.sparkContext.textFile("avocado.csv")

    import spark.implicits._

    val inputDf= input.map(rec=> {
      var temp = rec.split(",")
      Avocado(Date.valueOf(temp(1)),temp(2).toDouble,temp(3).toDouble,temp(4).toDouble,temp(5).toDouble,temp(6).toDouble,temp(7).toDouble,temp(8).toDouble,temp(9).toDouble,temp(10).toDouble,temp(11),temp(12),temp(13))
    }).toDF()

//    inputDf.filter($"TotalVolume">1000000 && $"region".notEqual("Albany")).collect().foreach(println)
   var totalVolumePerRegion=inputDf.groupBy("region").sum("TotalVolume").withColumnRenamed("sum(TotalVolume)","TotalVolume")

    totalVolumePerRegion.sort("TotalVolume").collect().foreach(println)
  }
  case class Avocado(Date: Date,AveragePrice:Double,TotalVolume:Double,_4046:Double,_4225:Double,_4770: Double,TotalBags:Double,SmallBags: Double,LargeBags: Double,XLargeBags: Double,_type: String, year: String,region: String)
}

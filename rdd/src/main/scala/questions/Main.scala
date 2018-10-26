package questions

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Main {

    def main(args: Array[String]) = {
	
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val spark = SparkSession.builder
          .master("local[8]")
          .appName("main")
          .config("spark.driver.memory", "5g")
          .getOrCreate()

        val path = getClass().getResource("/allCountries.txt").toString
        val processor = new GeoProcessor(spark,path)
        //println(new File((getClass.getResource("/allCountries.txt").getFile)).length)
        //val file = new File((getClass.getResource("/socialgraph.dat").getFile))

        //example for printing
        val filtered = processor.filterData(processor.file)

        //processor.mostCommonWords(filtered).take(10).foreach(x=>print(x))

        //filtered.take(100).foreach(x => println(x.mkString("\t")))

        //filtered.filter(_(8)=="FI").foreach(x => println(x.mkString("\t")))

        //processor.filterElevation("TR", filtered).take(10).foreach(x => print(x + " "))
        //val countryCodes = spark.sparkContext.textFile(getClass().getResource("/coutrycodes.csv").toString).map(x=>x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
        //countryCodes.foreach(x=> println(x(0)+"\t\t\t"+x(1)))
        print(processor.mostCommonCountry(filtered,getClass().getResource("/coutrycodes.csv").toString))
        //print(processor.hotelsInArea(37.80626, -122.41628))
        //stop spark
        //processor.loadSocial(getClass.getResource("/socialgraph.dat").toString)
        spark.stop()
    }
}

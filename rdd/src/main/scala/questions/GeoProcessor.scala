package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


import scala.math._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import scala.util.matching.Regex


/** GeoProcessor provides functionalites to
* process country/city/location data.
* We are using data from http://download.geonames.org/export/dump/
* which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
*
* @param spark reference to SparkSession
* @param filePath path to file that should be modified
*/
class GeoProcessor(spark: SparkSession, filePath:String) {

    //read the file and create an RDD
    //DO NOT EDIT
    val file = spark.sparkContext.textFile(filePath)

    /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
    def filterData(data: RDD[String]): RDD[Array[String]] = {
        /* hint: you can first split each line into an array.
        * Columns are separated by tab ('\t') character.
        * Finally you should take the appropriate fields.
        * Function zipWithIndex might be useful.
        */
        data.map(line => line.split('\t').zipWithIndex.collect{
           case (x, i) if i ==1 || i == 8  || i == 16 => x})
        // Collect is not best practice on big RDD's, however, the code below doesnt run
        //data.map(line =>Array(line.split("\t")(0),line.split("\t")(5), line.split("\t")(16)))
    }


    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String, data: RDD[Array[String]]): RDD[Int] = {
        data.filter(x => x(1) == countryCode).map{x => x(2).toInt}
    }



    /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
    def elevationAverage(data: RDD[Int]): Double = {
        data.sum() / data.count().toDouble
    }

    /** mostCommonWords calculates what is the most common
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '.
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
    def mostCommonWords(data: RDD[Array[String]]): RDD[(String,Int)] = {
        // Extract the 'name' column from RDD[Array[String]]
        // and split the text with delimiter ' ' then flatten the RDD
        val words = data.map(x=>x(0).split(' ')).flatMap(x=>x)
        // Count the words inside the RDD[String] then sort by DESC
        // .sortBy(_._2, false) -> false means sort DESC
        words.groupBy(x=>x).map(x=>(x._1,x._2.size)).sortBy(_._2, ascending = false)
    }

    /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
    def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
        // .groupBy(countryCode) and sortBy count in DESC order, get count of the first element
        val countries = data.groupBy(x=>x(1)).map(x=>(x._1,x._2.size))
        countries.foreach(x=>println("$$$->\t"+x))
        val mostCommonCountry = data.groupBy(x=>x(1)).map(x=>(x._1,x._2.size)).max(){new Ordering[(String, PartitionID)] {
              override def compare(x: (String, PartitionID), y: (String, PartitionID)): PartitionID =
                  Ordering[PartitionID].compare(x._2,y._2)
        }}
        // open the country codes file
        val countryCodeFile = spark.sparkContext.textFile(path)
        // split the lines with comma out of quotes (discard commas between quotes)
        val countryCodes = countryCodeFile.map(x=>x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
        // match country code and return the corresponding country name
        val found = countryCodes.filter(x=>x(1)==mostCommonCountry._1)
        if(found.isEmpty()){
            ""
        } else {
            found.first()(0)
        }
    }

//
    /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
    def hotelsInArea(lat: Double, long: Double): Int = {
        //spark.sparkContext.textFile("/home/tolgahan/Downloads/rdd/src/test/resources/sample.txt").map(line => line.split('\t')).filter(x=>x(1).matches("(?i).*hotel.*"))
        val allHotels = file.map(line => line.split('\t')).filter(x=>x(1).matches("(?i).*hotel.*"))
        // Create a filtering func for distance between two coordinate
        val isInTheDistance = (x: Array[String]) => {
            val dLat=(lat - x(4).toDouble).toRadians
            val dLon=(long - x(5).toDouble).toRadians
            val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat.toRadians) * cos(x(4).toDouble.toRadians)
            val c = 2.0 * asin(sqrt(a))
            6372800 * c <= 10000.0
        }
        val hotelsInRange = allHotels.filter(isInTheDistance)
        hotelsInRange.count.toInt
    }

    //GraphX exercises

    /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/latest/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
    def loadSocial(path: String): Graph[Int, Int] = {
        val file = spark.sparkContext.textFile(path).distinct
        val allNumbers = new Regex("[0-9]+")
        val eRDD = file.flatMap({ x =>
            val found = allNumbers.findAllIn(x).toVector
            if (found.size == 2) {
                Array((found(0).toInt, found(1).toInt))
            }
            else {
                None
            }
        }).map(x => Edge(x._1, x._2, 1))

        val vRDD = eRDD.map(x => (x.srcId, x.srcId.toInt)).distinct.union(eRDD.map(x => (x.dstId, x.dstId.toInt)).distinct)

        Graph(vRDD, eRDD)
    }

    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int, Int]): Int = {
        graph.ops.outDegrees.max() {
            new Ordering[(VertexId, PartitionID)] {
                override def compare(x: (VertexId, PartitionID), y: (VertexId, PartitionID)): PartitionID =
                    Ordering[PartitionID].compare(x._2, y._2)
            }
        }._1.toInt
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
    def pageRankHighest(graph: Graph[Int, Int]): Int = {
        graph.pageRank(0.0001).vertices.max() {
            new Ordering[(VertexId, Double)] {
                override def compare(x: (VertexId, Double), y: (VertexId, Double)): PartitionID =
                    Ordering[Double].compare(x._2, y._2)
            }
        }._1.toInt
    }

}
/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "TOLGAHAN"
}
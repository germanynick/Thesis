import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object Cooc {

    val WINDOW_SIZE = 6
    case class CoWord(first:String, second:String, distance: Int)
    
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Buil Cooc Word")
        val sc = new SparkContext(conf)
        
        val textFile = sc.textFile("/user/duc/sample.tok.txt")
        val lines = textFile.map(line => preProcessLine(line).split("[ ]+"))
        
        val coMap = lines.flatMap(x => parseCoWord(x)).map(x => ((x.first, x.second) , (1, x.distance)))
        //coMap.take(10).foreach(println)
        val coReduce = coMap.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
        //coReduce.take(10).foreach(println)
        
        val coMax = coReduce.map(x => (x._1._1, x._1._2, x._2._1, x._2._2/ x._2._1.toDouble)).collect.sortWith(_._3 > _._3)
        coMax.take(10).foreach(println)
    }
    def preProcessLine(line: String): String = {
        return line.replaceAll("[,]+|[.]+|[(]+|[)]+", "")
    }
    def parseCoWord(sentence: Array[String]):ArrayBuffer[CoWord]= {
        var co = ArrayBuffer[CoWord]()
        for (i <- 0 to (sentence.length -2)) {
            breakable {
                for (j <- i+1 to (sentence.length -1)) {
                    if (j - i > WINDOW_SIZE)
                        break;
                    co += CoWord(sentence(i), sentence(j), j-i)
                }
            }
        }   
        return co;
    }
    
    
}

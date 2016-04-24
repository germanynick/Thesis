import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark._
import scala.util.control.Breaks._

val textFile = sc.textFile("/user/duc/sample.tok.txt")
val lines = textFile.map(line =>line.replaceAll("[,]+|[.]+|[(]+|[)]+", "").split("[ ]+"))
//-- count word 
val counts = textFile.flatMap(line =>line.replaceAll("[,]+|[.]+|[(]+|[)]+", "").split("[ ]+")).map(word => (word, 1)).reduceByKey(
_ + _);

val WINDOWSIZE = 6;
case class CoWord(first: String, second:String)

def parseCoWord(sentence: Array[String]):ArrayBuffer[CoWord] = {
    var co = ArrayBuffer[CoWord]()
    for (i <- 0 to (sentence.length - 2)) {
    breakable {
        for (j <- i+1 to (sentence.length -1)) {
            if (j - i > WINDOWSIZE)
                break
            co += CoWord(sentence(i), sentence(j));
        }
    }
    }
    return co;
}

//-- count co-occurence word
val coMap = lines.flatMap(x => parseCoWord(x)).map(x => (x, 1))
val coReduce = coMap.reduceByKey(_ + _)

//-- sort
//val coMax = coReduce.collect.sortWith(_._2 >_._2)

// mapping word
val vertices = counts.collectAsMap()
val edges = coReduce.map(x => (x._1.first, x._1.second, x._2))

val CoocWord = edges.map(x => (x._1, x._2, vertices(x._1), vertices(x._2), x._3))

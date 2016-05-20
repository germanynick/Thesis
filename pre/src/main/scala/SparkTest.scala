import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkTest {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Spark Test")
        val sc = new SparkContext(conf)
        
        println("Test run Scala")
        Neo4jTest.Test()
    }
}

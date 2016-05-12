import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

object GenerateCSVFiles {
    def main(args: Array[String]) {
        
         // Read in the CSV File
        var crimeFile = System.getenv("CSV_FILE")
        
        if (crimeFile == null || !new File(crimeFile).exists()) {
            //throw new RuntimeExeption("Cannot find CSV file [" + crimeFile  + "]")
        }
        println("Using %s".format(crimeFile))
        
        val conf = new SparkConf().setAppName("Chicago Crime Dataset")
        val sc = new SparkContext(conf)
       
        // Load CSV to Sources
        val crimeData = sc.textFile(crimeFile).cache()
        val withoutHeader = dropHeader(crimeData)
        generateFile("/tmp/crimes.csv", withoutHeader,columns => Array(columns(0), "Crime",
            columns(2), columns(6)), "id:ID(Crime), :LABEL, date, desciption", false)        
    }
    
    def dropHeader(data:RDD[String]):RDD[String] = {
        data.mapPartitionsWithIndex((idx, lines) => {
            if (idx == 0) { lines.drop(1) }
            lines
        })
    }
    def generateFile(file: String, withoutHeader: RDD[String], fn:Array[String] =>
        Array[String], header:String, distinct:Boolean = true, separator: String = ",") = {
        
        FileUtil.fullyDelete(new File(file))
        
        val tmpFile = "/tmp/" + System.currentTimeMillis() + "-" + file 
        val rows: RDD[String] = withoutHeader.mapPartitions(lines => {
            val parser = new CSVParser(',')
            lines.map(line => {
                val columns = parser.parseLine(line)
                fn(columns).mkString(separator)
            })
        })
        if (distinct) rows.distinct() saveAsTextFile tmpFile else rows.saveAsTextFile(tmpFile)
        merge(tmpFile, file, header)
    }
    
    def merge(srcPath:String, dstPath:String, header:String) :Unit =  {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        MyFileUtil.copyMergeWithHeader(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig,header)
    
    }
}

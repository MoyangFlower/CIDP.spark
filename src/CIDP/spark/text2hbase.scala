package CIDP.spark

import java.io.File
import java.lang
import java.util.Collections
import scala.io.Source
import collection.mutable.Map
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}


object text2hbase {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("text2hbase")
      .setMaster("spark://127.0.0.1:7077")
    //.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val tablename = "earthquakedata"
    val src = "/home/hadoop/txt_data"

    //sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "192.168.200.221,192.168.200.225,192.168.200.222,192.168.200.223,192.168.200.228")
    sc.hadoopConfiguration.addResource("/home/hadoop/hadoop-2.8/etc/hadoop/core-site.xml")
    sc.hadoopConfiguration.addResource("/home/hadoop/hadoop-2.8/etc/hadoop/hdfs-site.xml")
    //sc.hadoopConfiguration.set("hbase.zookeeper.quorum ", "asterix-2,asterix-4,asterix-6,asterix-8,asterix-10")
    //sc.hadoopConfiguration.set("zookeeper.znode.parent", "/hbase")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)


    val conf = HBaseConfiguration.create()

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)


    //获取文件目录下的所有txt文件
    val files = new File(src).listFiles()
    //Collections.shuffle(files)
    //文件名 stationID+pointID+itemID+sample || 03002A41120
    //文件行 date,time,family,column,value|date2,time2,family2,column2,value2 || 19800426,10:26:00,second,10*3600+26*60,56.2|
    // 04001g41120120100909,0000,Min,0000,52.9330
    for(file <- files){
      System.out.println(file)
      val lines = Source.fromFile(file).getLines()
      lines foreach(line => {
        val RDD = sc.makeRDD(Array(line))
        System.out.println("the RDD",RDD)
        val data_rdd = RDD.flatMap{s => s.split("\\|")}.map(_.split(",")).map{ x =>{

          val put = new Put(Bytes.toBytes(x(0)))
          put.addColumn(Bytes.toBytes(x(1)),Bytes.toBytes(x(2)),Bytes.toBytes(x(3)))
          (new ImmutableBytesWritable,put)
        }
        }
        data_rdd.saveAsHadoopDataset(jobConf)
        //data_rdd.saveAsNewAPIHadoopDataset(jobConf)
      })
    }
  }
}
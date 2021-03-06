package CIDP.spark

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{HTable, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object import2hbase {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("import2hbase-out-loop")
      .setMaster("spark://asterix-1:7077")
    //.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val tablename = "PecursordData"
    val src = "/home/hadoop/txt_data"
    val filename = "/home/hadoop/txt_data_log.txt"
    sc.hadoopConfiguration.addResource("/home/hadoop/hadoop-2.7.6/etc/hadoop/core-site.xml")
    sc.hadoopConfiguration.addResource("/home/hadoop/hadoop-2.7.6/etc/hadoop/hdfs-site.xml")
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum ", "asterix-2,asterix-6,asterix-8,asterix-10")
    sc.hadoopConfiguration.set("zookeeper.znode.parent", "/hbase")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)


    val conf = HBaseConfiguration.create()

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //获取文件目录下的所有txt文件
    val files = new File(src).listFiles()

    //文件名 stationID+pointID+itemID+sample || 03002A41120
    //文件行 date,time,family,column,value|date2,time2,family2,column2,value2 || 19800426,10:26:00,second,10*3600+26*60,56.2|
    // 04001g41120120100909,0000,Min,0000,52.9330

    for(file <- files){
      System.out.println(file)
      val lines = Source.fromFile(file).getLines()
      lines foreach(line => {
        val RDD = sc.makeRDD(Array(line))
        System.out.println("the RDD",RDD)
        // RDD 尽可能大，才能发挥spark计算能力。太小的RDD会在提交任务上花费很多时间。
        val data_rdd = RDD.flatMap(s => s.split("\\|")).repartition(8).map(_.split(",")).map{ x =>{
          val put = new Put(Bytes.toBytes(x(0)))
          x(2).split(" ") foreach (column => {
            try {
              put.addColumn(Bytes.toBytes(x(1)), Bytes.toBytes(column.substring(0, 4)), Bytes.toBytes(column.substring(5)))
            }catch{
              case e:StringIndexOutOfBoundsException => {
                println(e)
                try {
                  val writer = new BufferedWriter(new FileWriter(new File(filename), true))
                  writer.write(x(2))
                  writer.close()
                } catch {
                  case e: Exception =>
                      e.printStackTrace()
                }
              }
            }
          })
          (new ImmutableBytesWritable,put)
        }
        }
        data_rdd.saveAsHadoopDataset(jobConf)

        //data_rdd.saveAsNewAPIHadoopDataset(jobConf)
      })
    }
  }
}
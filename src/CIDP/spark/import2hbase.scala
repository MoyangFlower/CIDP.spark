package CIDP.spark

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

object import2hbase {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("inputdata")
      .setMaster("spark://127.0.0.1:7077")
    //.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val tablename = "earthquakedata"


    //sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "192.168.200.221,192.168.200.225,192.168.200.222,192.168.200.223,192.168.200.228")
    sc.hadoopConfiguration.addResource("/home/hadoop/hadoop-2.7.6/etc/hadoop/core-site.xml")
    sc.hadoopConfiguration.addResource("/home/hadoop/hadoop-2.7.6/etc/hadoop/hdfs-site.xml")
    //sc.hadoopConfiguration.set("hbase.zookeeper.quorum ", "asterix-2,asterix-4,asterix--6,asterix-8,asterix-10")
    //sc.hadoopConfiguration.set("zookeeper.znode.parent", "/hbase")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //val sc=new SparkContext(sparkConf)
    //val tableName="student"
    // sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    val conf = HBaseConfiguration.create()

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    //构建新纪录

    val dataRDD = sc.makeRDD(Array("03002,9,4112,18.7", "04003,2,9130,24","13002,4,4113,95.4", "14003,A,9110,48"))
    val rdd = dataRDD.map(_.split(",")).map { x => {
      val put = new Put(Bytes.toBytes(x(0)+x(1)+x(2))) //行健的值   Put.add方法接收三个参数：列族,列名,数据
      put.addColumn(Bytes.toBytes("Day"), Bytes.toBytes("00"), Bytes.toBytes(x(3))) //Day:00列的值
      put.addColumn(Bytes.toBytes("Day"), Bytes.toBytes("10"), Bytes.toBytes(x(3))) //Day:10列的值
      put.addColumn(Bytes.toBytes("Day"), Bytes.toBytes("20"), Bytes.toBytes(x(3))) //Day:20列的值
      (new ImmutableBytesWritable, put) ////转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
    }
    }
    rdd.saveAsHadoopDataset(jobConf)
  }
}
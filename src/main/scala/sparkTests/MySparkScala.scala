package sparkTests

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds


import org.apache.spark.storage.StorageLevel._


import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder





object MySparkScala extends App{


  //simpleTest
  simpleTestSubmit

  def simpleTest() {
    /*val conf = new SparkConf().
    setMaster("spark://stationrotation.corp.ne1.yahoo.com:7077").setAppName("My app")
    */
    val conf = new SparkConf().setMaster("local").setAppName("My within eclipse app")
    val sc = new SparkContext(conf)
    val data = 1 to 1000
    val distData = sc.parallelize(data)
    println("Storage level of this RDD distData is " + distData.getStorageLevel)
    distData.filter(_ < 10).collect().foreach(println)
    println(distData.toDebugString)
    sc.stop()
  }


  def simpleTestSubmit() {
    /*val conf = new SparkConf().
    setMaster("spark://stationrotation.corp.ne1.yahoo.com:7077").setAppName("My app")
    */
    val conf = new SparkConf().setAppName("My app")
    val sc = new SparkContext(conf)
    //sc.addJar("/Users/vmishra/Jan21-2016-Onwards/Untitled.jar")

    val data = 1 to 1000
    val distData = sc.parallelize(data)
    distData.filter(_ < 10).collect().foreach(println)
    println(distData.toDebugString)
    Thread.sleep(10000)
    sc.stop()

  }


}
package com.luke

import java.io.{File, FileOutputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by lukeforehand on 12/31/15.
 */
object FormatImageInputs {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("FormatImageInputs")
    if (args.length > 5) {
      // grab param for master "local[4]"
      conf.setMaster(args(5))
    }
    val sc = new SparkContext(conf)

    val urls = args(0)
    val words = args(1)
    val urlsOutput = args(2)
    val labelsOutput = args(3)
    val numFiles = args(4).toInt

    // broadcast sequenced labels
    val labelsBroadcast = sc.broadcast(sc.textFile(words).map {
      line => {
        val parts = line.split("\t")
        (parts(0), parts(1))
      }
    }.collectAsMap().map {
      var count = 0
      map => {
        val r = (map._1, (count, map._2))
        count = count + 1
        r
      }
    })

    // save urls associated with label sequence
    sc.textFile(urls).map {
      line => {
        val parts = line.split("\t")
        val url = parts(1)
        url + "\t" + labelsBroadcast.value.get(parts(0).split("_")(0)).get._1
      }
    }.coalesce(numFiles).saveAsTextFile(urlsOutput)

    // save sequenced labels
    FileUtils.deleteQuietly(new File(labelsOutput))
    val out = new FileOutputStream(new File(labelsOutput))
    for (line <- labelsBroadcast.value.toSeq.sortBy(_._2._1)) {
      IOUtils.write(line._2._2 + "\n", out)
    }
    IOUtils.closeQuietly(out)

  }

}



package com.cloudera.sa.example.errorsNRT

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql._
import org.apache.spark.streaming.flume._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._
import java.io._


case class HTTPEvent(code: String, count: String, ms: String)
case class Config(
  master: String = "yarn-client",
  out : String = "",
  agg: String = "",
  cp: String = "",
  flumeHost: String = "localhost",
  flumePort: Int = 7777,
  batchSeconds: Int = 15,
  slideSeconds: Int = 300,
  windowSeconds: Int = 300,
  verbose: Boolean = false
)

class Interrogator {

  val httpFlumeHeaderRegex = """.host.[^ ]*. uuid.[^ ]*. timestamp.[^ ]*."""
  val validIpRegex = """[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"""
  val validHostnameRegex = """[^ ]*\.[^ ].*"""
  val validIpORHostnameRegex = """[^ ]*\.[^ ].*"""
  val validDateRegex = """\[[0-3]*[0-9]\/[a-zA-Z]{3}\/[0-9]{4}\:[0-9]{2}\:[0-9]{2}\:[0-9]{2} .{5}\]"""
  val validPortRegex = """\:[0-9]{2}"""
  val validReqRegex = """\"[A-Z]{3}.*HTTP\/.{3}\""""
  val validCodeRegex = """[1-5][0-5][0-9]"""

  def getStatusCode(body : String) : String = {
    val list = List (
      ".*",
      validIpORHostnameRegex,
      " ",
      validDateRegex,
      ".* ",
      validReqRegex,
      " ",
      "(" + validCodeRegex + ")" + " .*"
    )

    val regexUpToStatusCode = list.mkString("")

    val it = regexUpToStatusCode.r.findAllIn(body)
    val a = regexUpToStatusCode.r.findAllIn(body).toArray
    if(a.size > 1) {
      //throw new IllegalArgumentException("Multiple matches for HTTP status code. HTTP log message was" + body)
      return "998"
    }
    if(a.size < 1) {
      //throw new IllegalArgumentException("No match for HTTP status code. HTTP log message was" + body)
      return "999"
    }

    it.matchData.next().group(1)
  }
}

object ErrorsNRT {

  val parser = new scopt.OptionParser[Config]("ErrorsNRT") {

    head("webPage Errors NRT analyser", "1.0.0")

    opt[String]('m', "master") valueName ("[ yarn-client | local[5] ]") action { (x, c) =>
      c.copy(master = x)
    } text ("master is used to define the Spark-Context.\n")

    opt[String]('o', "outfile") valueName ("<outfile>") action { (x, c) =>
      c.copy(out = x)
    } text ("outfile defines the HDFS location where the events within the window get written to\n")

    opt[String]('a', "aggfile") required () valueName ("<aggfile>") action { (x, c) =>
      c.copy(agg = x)
    } text ("aggfile defines the _required_ HDFS location where events counts grouped by error codes within the window get written to\n")

    opt[String]('a', "cpfile") required () valueName ("<cpfile>") action { (x, c) =>
      c.copy(cp = x)
    } text ("cpfile defines the _required_ HDFS location where Spark will materialize checkpoints for stateful transformations to\n")

    opt[String]('h', "flumeHost") valueName ("<flume-host>") action { (x, c) =>
      c.copy(flumeHost = x)
    } text ("defines the host running the flume spark sink that the receiver will connect to. Defaults to 'localhost'\n")

    opt[Int]('p', "flumePort") valueName ("<flume-port>") action { (x, c) =>
      c.copy(flumePort = x)
    } text ("defines the port running the flume spark sink that the receiver will connect to. Defaults to 7777\n")

    opt[Boolean]('v', "verbose") valueName ("<verbose>") action { (x, c) =>
      c.copy(verbose = x)
    } text ("Verbosity\n")

    opt[Int]('s', "batchSeconds") valueName ("<batch-interval-seconds>") action { (x, c) =>
      c.copy(batchSeconds = x)
    } text ("defines the interval of micro-batches\n")

    opt[Int]('l', "slideSeconds") valueName ("<slide-interval-seconds>") action { (x, c) =>
      c.copy(slideSeconds = x)
    } text ("defines the interval at which the sliding window of events gets written to HDFS\n")

    opt[Int]('l', "windowSeconds") valueName ("<window-interval-seconds>") action { (x, c) =>
      c.copy(windowSeconds = x)
    } text ("defines the amount of time that the sliding window reaches back to\n")
  }

  def main(args: Array[String]) {

    var myConfig: Config = new Config()

    val log = Logger.getRootLogger()


    parser.parse(args, myConfig) map { config =>

      log.setLevel(Level.INFO)
      log.info(">>> config.master           : " + config.master)
      log.info(">>> config.out              : " + config.out)
      log.info(">>> config.agg              : " + config.agg)
      log.info(">>> config.cp               : " + config.cp)
      log.info(">>> config.flumeHost        : " + config.flumeHost)
      log.info(">>> config.flumePort        : " + config.flumePort)
      log.info(">>> config.batchSeconds     : " + config.batchSeconds)
      log.info(">>> config.slideSeconds     : " + config.slideSeconds)
      log.info(">>> config.windowSeconds    : " + config.windowSeconds)
      log.info(">>> config.verbose          : " + config.verbose)

      myConfig = config

    } getOrElse {
      // arguments are bad, usage message will have been displayed
      usage()
      System.exit(1)
    }

    def usage(): Unit = {
      println("Usage: --help shows you the full parameter list.")
    }


    val microBatchSeconds = myConfig.batchSeconds
    val windowSeconds = myConfig.windowSeconds
    val slideSeconds = myConfig.slideSeconds
    val cpfile = new File(myConfig.cp)
    val sparkConf = new SparkConf().
      setMaster(myConfig.master).
      setAppName("ErrorsNRT").
      set("spark.cleaner.ttl", "120000")
    val sc = new SparkContext(sparkConf)


    var outfile : File = null
    if(myConfig.out.length >= 1) {
      outfile = new File(myConfig.out)
    }

    // Create a StreamingContext w 1s batch from SparkConf -> produces warning since we now have two spark contexts
    val sscConf = sc.getConf
    sscConf.set("spark.streaming.blockInterval", "100")
    val ssc = new StreamingContext(sc, Seconds(myConfig.batchSeconds))
    ssc.checkpoint(cpfile.getAbsolutePath)
    // Create a DStream using data received after connecting to port 7777 on the local machine
    val flumeStream = FlumeUtils.createPollingStream(ssc, myConfig.flumeHost, myConfig.flumePort)

    // These are the key/value (error-code/log-body) pairs as we want to deal with them, derived from the flume event stream
    if(myConfig.out.length >= 1) {
      val microBatch = flumeStream.map(e => (new String(new Interrogator().getStatusCode(
        new String(e.event.getBody.array()))).toInt,
        new String(e.event.getBody.array())))
      val reportedBatch = microBatch.window(Seconds(windowSeconds), Seconds(slideSeconds))
      reportedBatch.saveAsTextFiles(outfile.getAbsolutePath)
    }

    // These are the aggregates (error-code/count) that we want to report
    // map the events into key/value pairs -> (errorcode, 1)
    val microAgg = flumeStream.map(e => (new String(new Interrogator()
      .getStatusCode( new String(e.event.getBody.array()))).toInt, 1))

    // reduces all events that occured within the given window of time
    // and produces a new DStream on that time window
    // reduction is by adding new events (+1 per event)
    // and substracting old events (-1 per event)
    val reportedAgg = microAgg.reduceByKeyAndWindow(
      {(a,b) => a + b},
      {(a,b) => a - b},
      Seconds(windowSeconds),
      Seconds(slideSeconds))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    reportedAgg.foreachRDD((rdd, time) => {
      val rowsDF = rdd.map(x => HTTPEvent(x._1.toString, x._2.toString, time.toString().stripSuffix(" ms"))).toDF()
      rowsDF.write.mode(org.apache.spark.sql.SaveMode.Append).save(myConfig.agg)
    })

    ssc.start()

    ssc.awaitTermination()

  }
}

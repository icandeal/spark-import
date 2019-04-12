package com.etiantian

import java.io.FileInputStream
import java.sql.DriverManager
import java.util.Properties

import scala.util.Try

object RdbmsImporter {

  val logger = LogManager.getLogger("RdbmsImporter")

  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.load(new FileInputStream(args(0)))
//    prop.load(new FileInputStream("E:\\Project\\common-util\\rdbms-import\\src\\main\\scala\\com\\etiantian\\bigdata\\tmp.properties"))
    val url = prop.getProperty("url")
    val driver = prop.getProperty("driver")
    val username = prop.getProperty("username")
    val password = prop.getProperty("password")
    var table = prop.getProperty("table")
    val where = prop.getProperty("where")
    var cols = prop.getProperty("cols")
    val addCols = prop.getProperty("addCols")
    val query = prop.getProperty("query")
    val splitBy = prop.getProperty("splitBy")
    val pm = prop.getProperty("partitionNum")
    val lb = prop.getProperty("lowerbound")
    val ub = prop.getProperty("upperbound")
    val fetchSize = prop.getProperty("fetchSize")
    val batch = prop.getProperty("batch")
    val executors = prop.getProperty("executors")
    val hiveTable = prop.getProperty("hiveTable")
//    val hivePartition = prop.getProperty("hivePartition")
//    val overwrite = prop.getProperty("overwrite")


    if (table == null || table.trim.length < 1) {
      table = "("+query+") as tmp"
    }

    Class.forName(driver)
    val conn = DriverManager.getConnection(url, username, password)
    val statement = conn.createStatement()
    val resultSet = statement.executeQuery(s"select min($splitBy), max($splitBy), count(*) from $table where length(trim($splitBy))>0")
    resultSet.next()
    val size = resultSet.getString(3)
    var lowerbound = if (Try(resultSet.getString(1).toLong).isSuccess) resultSet.getString(1) else "1"
    var upperbound = if (Try(resultSet.getString(2).toLong).isSuccess) resultSet.getString(2) else size

    var p = size.toLong / batch.toLong
    if (p == 0)
      p = 1
    val q = (upperbound.toLong - lowerbound.toLong) / batch.toLong
    p = if (q / p > 1000) p * 55  else p
    if (p > executors.toLong * 1250)
      p = executors.toLong * 1250
    var partitionNum = if (p < executors.toLong) executors.toLong else p + (executors.toLong - p % executors.toLong)

    if (lb != null && lb.trim.length > 0) {
      lowerbound = lb
    }
    if (ub != null && ub.trim.length > 0) {
      upperbound = ub
    }
    if (pm != null && pm.trim.length > 0) {
      partitionNum = pm.toLong
    }

    println(s"###############  lowerbound = $lowerbound  ######################")
    println(s"###############  upperbound = $upperbound  ######################")
    println(s"###############  partitionNum = $partitionNum  ##################")
    println(s"###############  batch = $batch  ################################")

    val sparkConf = new SparkConf().setAppName(s"common-util:RdbmsImporter:$hiveTable")
//      .setMaster("local[5]")
    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("WARN")
    val sqlContext = new HiveContext(sc)

    val t1 = System.currentTimeMillis()
    var df = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> url,
        "driver"-> driver,
        "user"-> username,
        "password"-> password,
        "dbtable"-> table,
        "partitionColumn"-> splitBy,
        "numPartitions" -> partitionNum.toString,
        "lowerBound"-> lowerbound,
        "upperBound"-> upperbound,
        "fetchSize"-> fetchSize
      )
    ).load()

//    df.schema.foreach(sf => {
//      val col = sf.name
//      val dataType = sf.dataType
//      df = df.withColumn(
//        col, if (dataType.isInstanceOf[TimestampType]) df(col).cast(StringType) else df(col)
//      ).withColumnRenamed(col, col.toLowerCase())
//    })

    df.columns.foreach(col => {
      df = df.withColumnRenamed(col, col.toLowerCase())
    })

    if (where != null && where.trim.length > 0){
      df = df.filter(where)
    }
    if (cols != null && cols.trim.length >0) {
      df.registerTempTable("tmpTbl")
      cols = cols.toLowerCase()
      df = sqlContext.sql(s"select $cols from tmpTbl")
    }
    if (addCols != null && addCols.trim.length >0) {
      val oldCols = df.columns.mkString(",")
      df.registerTempTable("tmpTbl")
      val newCols = addCols.toLowerCase()
      println(s"select $oldCols, $newCols from tmpTbl")
      df = sqlContext.sql(s"select $oldCols, $newCols from tmpTbl")
    }
    if (partitionNum > 400) {
      df = df.repartition(400)
    }
    df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("###################  Import record:"+df.count +". Time spend:" + (System.currentTimeMillis() - t1) + "  partition size:"+df.rdd.partitions.size + "####################")
//    df.show()
    df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(hiveTable)
    sc.stop()
  }
}

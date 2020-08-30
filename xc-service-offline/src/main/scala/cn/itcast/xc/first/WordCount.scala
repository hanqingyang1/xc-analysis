package cn.itcast.xc.first

import cn.itcast.xc.common.EtlEnvironment
import org.apache.spark.sql.SparkSession

/**
 * wordcount
 */
object WordCount {
  /**
   * 初始化SparkSession
   */
  val spark: SparkSession = EtlEnvironment.getSparkSession(this.getClass.getSimpleName)

  /**
   * 主函数, 程序入口, 程序逻辑
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sc = spark.sparkContext

    // 读取hdfs文件
    val input = sc.textFile("hdfs://xc-online-hadoop:9000/word.txt")

    // 进行计算统计单词个数
    val counts = input
      // 单词分割, 这里使用'\t'
      .flatMap(line => line.split("\t"))
      // 单词计数( 每个都是1 )
      .map(word => (word, 1))
      // 合并单词的计数
      .reduceByKey(_ + _)
      // 降序排序
      .sortBy(_._2, false)

    // 输出保存到文件(hdfs)
    //    counts.saveAsTextFile("hdfs://xc-online-hadoop:9000/word_count")
    // 本地模式
    //    counts.saveAsTextFile("./word_count")
    // 控制台输出
    counts.collect().foreach(println(_))

    // 关闭资源
    sc.stop()
    spark.stop()
  }
}

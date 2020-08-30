package cn.itcast.xc.utils

import org.apache.spark.sql.SparkSession

/**
 * <P>
 * 和课程相关的工具类
 * </p>
 *
 */
object SubjectUtils {

  /**
   * 学生人数 及涨幅
   *
   * @param spark   spark
   * @param current 当前日期区间
   * @param last    要对比的日期区间
   * @return
   */
  def subjectLearnSql(spark: SparkSession, current: Array[String], last: Array[String]) = {
    spark.sql(
      s"""
         |SELECT a.course_category_dim_id, a.user_num,
         |    ifnull((a.user_num-b.user_num)/b.user_num, 1.0) as user_percent
         |from
         |    (SELECT course_category_dim_id, count(user_dim_id) user_num
         |    FROM data_course.learning_course_fact
         |    WHERE concat_ws('-',years,months,days) BETWEEN '${current(0)}' AND '${current(1)}'
         |    GROUP BY course_category_dim_id) as a
         |LEFT JOIN
         |    (SELECT course_category_dim_id, count(user_dim_id) user_num
         |    FROM data_course.learning_course_fact
         |    WHERE concat_ws('-',years,months,days) BETWEEN '${last(0)}' AND '${last(1)}'
         |    GROUP BY course_category_dim_id) as b
         |on a.course_category_dim_id = b.course_category_dim_id
         |""".stripMargin)
  }


  /**
   * 销售量 根据日期进行统计 及涨幅
   *
   * @param spark   spark
   * @param current 当前日期区间
   * @param last    要对比的日期区间
   * @return
   */
  def subjectBuyCountSql(spark: SparkSession, current: Array[String], last: Array[String]) = {
    spark.sql(
      s"""
         |SELECT a.course_category_dim_id  course_category_dim_id, a.sale_num sale_num,
         |    ifnull((a.sale_num-b.sale_num)/b.sale_num, 1.0) sale_percent
         |FROM
         |    (SELECT course_category_dim_id, sum(salesvolume) sale_num
         |    FROM data_course.course_buy_dwm
         |    WHERE concat_ws('-',years,months,days) BETWEEN '${current(0)}' and '${current(1)}'
         |    GROUP BY course_category_dim_id) as a
         |LEFT JOIN
         |    (SELECT course_category_dim_id, sum(salesvolume) sale_num
         |    FROM data_course.course_buy_dwm
         |    WHERE concat_ws('-',years,months,days) BETWEEN '${last(0)}' and '${last(1)}'
         |    GROUP BY course_category_dim_id) as b
         |on a.course_category_dim_id = b.course_category_dim_id
         |""".stripMargin)
  }

  /**
   * 销售额 根据日期进行统计 及涨幅
   *
   * @param spark   spark
   * @param current 当前日期区间
   * @param last    要对比的日期区间
   * @return
   */
  def subjectBuyAmountSql(spark: SparkSession, current: Array[String], last: Array[String]) = {
    spark.sql(
      s"""
         |SELECT a.course_category_dim_id  course_category_dim_id, a.sale_amount sale_amount,
         |    ifnull((a.sale_amount-b.sale_amount)/b.sale_amount, 1.0) amount_percent
         |FROM
         |    (SELECT course_category_dim_id, sum(sales) sale_amount
         |    FROM data_course.course_buy_dwm
         |    WHERE concat_ws('-',years,months,days) BETWEEN '${current(0)}' and '${current(1)}'
         |    GROUP BY course_category_dim_id) as a
         |LEFT JOIN
         |    (SELECT course_category_dim_id, sum(sales) sale_amount
         |    FROM data_course.course_buy_dwm
         |    WHERE concat_ws('-',years,months,days) BETWEEN '${last(0)}' and '${last(1)}'
         |    GROUP BY course_category_dim_id) as b
         |on a.course_category_dim_id = b.course_category_dim_id
         |""".stripMargin)
  }

}

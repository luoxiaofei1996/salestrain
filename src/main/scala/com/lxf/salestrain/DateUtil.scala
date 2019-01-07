package com.lxf.salestrain

import java.text.SimpleDateFormat
import java.util.Date


object DateUtil {
  //计算日期差
  def date2weekNum( dateStr: String): Int = {
    val sdf=new SimpleDateFormat("yyyyMMdd")
    val time = sdf.parse("20180501").getTime
    val time2 = sdf.parse(dateStr).getTime
    return ((((time - time2) / (1000 * 3600 * 24)).toInt - 1)/7).toInt


  }


}

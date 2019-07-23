package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Author: Wmd
  * Date: 2019/7/23 10:31
  */
case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long) {
  private val date = new Date(ts)
  val logDate: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
  val logHour: String = new SimpleDateFormat("HH").format(date)
  val logHourMinute: String = new SimpleDateFormat("HH:mm").format(date)

  override def hashCode(): Int = uid.hashCode

  override def equals(obj: scala.Any): Boolean = {

    obj match {
      case o:StartupLog => o.uid == uid
      case _ => false
    }
  }

}


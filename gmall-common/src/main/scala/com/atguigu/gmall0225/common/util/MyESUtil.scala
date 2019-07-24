package com.atguigu.gmall0225.common.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, DocumentResult, Index}

/**
  * Author: Wmd
  * Date: 2019/7/24 10:52
  */
object MyESUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _



  /**
    * 构建客户端工厂对象
    */
  def buildFactory(): Unit = {
    val config: HttpClientConfig = new HttpClientConfig.Builder(s"$ES_HOST:$ES_HTTP_PORT")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000)
      .build()
    factory = new JestClientFactory()
    factory.setHttpClientConfig(config)
  }

  /**
    * 获取客户端对象
    *
    * @return
    */
  def getClient(): JestClient = {
    if (factory == null) buildFactory()
    factory.getObject
  }


  def insertBulk(index: String, sources: Iterable[Any]) = {
    val bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
    sources.foreach(any => {
      bulkBuilder.addAction(new Index.Builder(any).build())
    })
    val client: JestClient = getClient()
    client.execute(bulkBuilder.build())
    closeClient(client)
  }


  /**
    * 关闭客户端
    *
    * @param client
    */
  def closeClient(client: JestClient) = {
    if (client != null) {
      try {
        client.shutdownClient()
      } catch {
        case e => e.printStackTrace()
      }
    }
  }

  def main(args: Array[String]): Unit = {
      //测试
    val source1 = Startup("mid777111", "uid222", "", "", "", "", "", "", "", "", "", 123124141)
    val source2 = Startup("mid777111", "uid222", "", "", "", "", "", "", "", "", "", 123124141)
    insertBulk("testbulk",source1::source2::Nil)
  }
}
case class Startup(mid: String,
                   uid: String,
                   appid: String,
                   area: String,
                   os: String,
                   ch: String,
                   logType: String,
                   vs: String,
                   var logDate: String,
                   var logHour: String,
                   var logHourMinute: String,
                   var ts: Long
                  ) {

}

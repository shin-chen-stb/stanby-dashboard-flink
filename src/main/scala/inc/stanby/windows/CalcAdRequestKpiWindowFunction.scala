
package inc.stanby.windows

import inc.stanby.schema.{AdTracking, AdTrackingRequestKpi}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcAdRequestKpiWindowFunction extends ProcessWindowFunction[AdTracking, AdTrackingRequestKpi, String, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");

  override def process(key: String, context: ProcessWindowFunction[AdTracking, AdTrackingRequestKpi, String, TimeWindow]#Context, input: lang.Iterable[AdTracking], out: Collector[AdTrackingRequestKpi]): Unit = {
    logger.info("Calc Ad Search Kpi Process Function been initialized")
    val inputList = input.asScala
    var adCT = 0
    var adRequestMatched = 0
    var totalCpc = 0
    var avgCpc = 0.0
    var bidCpc = 0.0
    var revenue = 0.0
    val time = inputList.head.getCreateDateTime
    for (in <- inputList) {
      if (in.getLogType.toString.equals("click")) {
        adCT += 1
        totalCpc += in.getContractCpc
        bidCpc += in.getBidCpc
        revenue += in.getContractCpc
      }
      if (in.getLogType.toString.equals("distribution")) {
        adRequestMatched = 1
      }
    }
    if (adCT > 0) {
      avgCpc = totalCpc / adCT
    }
    val adTrackingRequestKpi = AdTrackingRequestKpi.newBuilder
      .setRequestId(key)
      .setAdCT(adCT)
      .setAdRequest(1)
      .setAdRequestMatched(adRequestMatched)
      .setCpcTotal(totalCpc)
      .setCpc(avgCpc)
      .setRps(revenue)
      .setRpsMatched(revenue)
      .setTime(time)
      .build()
    out.collect(adTrackingRequestKpi)
  }
}

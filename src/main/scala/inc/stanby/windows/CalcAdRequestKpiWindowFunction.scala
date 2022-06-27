
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
    var clickCount = 0
    var totalCpc = 0
    var avgCpc = 0.0
    val time = inputList.head.getCreateDateTime
    for (in <- inputList) {
      println(in.toString)
      if (in.getLogType.toString.equals("click")) {
        clickCount += 1
        totalCpc += in.getContractCpc
      }
    }
    if (clickCount > 0) {
      avgCpc = totalCpc / clickCount
    }
    val adTrackingRequestKpi = AdTrackingRequestKpi.newBuilder
      .setRequestId(key)
      .setAdCTCount(CTCount)
      .setCpcTotal(totalCpc)
      .setCpcAvg(avgCpc)
      .setTime(time)
      .build()
    out.collect(adTrackingRequestKpi)
  }
}

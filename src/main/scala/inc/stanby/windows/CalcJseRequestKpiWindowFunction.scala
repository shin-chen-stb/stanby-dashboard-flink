
package inc.stanby.windows

import inc.stanby.schema.{JseTracker, JseTrackingRequestKpi}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcJseRequestKpiWindowFunction extends ProcessWindowFunction[JseTracker, JseTrackingRequestKpi, String, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");

  override def process(key: String, context: ProcessWindowFunction[JseTracker, JseTrackingRequestKpi, String, TimeWindow]#Context, input: lang.Iterable[JseTracker], out: Collector[JseTrackingRequestKpi]): Unit = {
    logger.info("Calc JSE Search Kpi Process Function been initialized")
    val inputList = input.asScala
    var eventCount = 0
    var organicImpression = 0
    var organicCT = 0
    var adImpression = 0
    var adCT = 0
    var relatedJobsCTCount = 0
    var jobDetailsImpressionCount = 0
    var organicAdCTRatio = 0
    var adCTRatio = 0
    var jobImpressionPerClick = 0.0
    var adImpressionPerClick = 0.0
    var organicClickPerImpression = 0.0
    var adClickPerImpression = 0.0
    val time = inputList.head.getTime
    for (in <- inputList) {
      println(in.toString)
      if (in.getEventType.toString.equals("jobImpression") && !in.getIsAd) {
        organicImpression += 1
      }
      if (in.getEventType.toString.equals("jobClick") && !in.getIsAd) {
        organicClick += 1
      }
      if (in.getEventType.toString.equals("jobImpression") && in.getIsAd) {
        adImpression += 1
      }
      if (in.getEventType.toString.equals("jobClick") && in.getIsAd) {
        adClick += 1
      }
      if (in.getEventType.toString.equals("relatedJobsClick")) {
        relatedJobsClickCount += 1
      }
      if (in.getEventType.toString.equals("jobDetailsImpression")) {
        jobDetailsImpressionCount += 1
      }
      eventCount += 1
    }

    if (organicClick > 0 && adClick > 0) {
      adClickRatio = adClick/(organicClick+adClick)
      organicAdClickRatio = organicClick/adClick
    }

    if (organicImpression > 0) {
      organicClickPerImpression = organicClick.toFloat / organicImpression
    }

    if (adImpression > 0) {
      adClickPerImpression = adClick.toFloat / adImpression
    }

    if (organicClick > 0) {
      jobImpressionPerClick = organicImpression.toFloat / organicClick
    }

    if (adClick > 0) {
      adImpressionPerClick = adImpression.toFloat / adClick
    }

    val jseTrackingRequestKpi = JseTrackingRequestKpi.newBuilder
      .setSearchRequestId(key)
      .setEventCount(eventCount)
      .setOrganicClick(organicClick)
      .setOrganicImpression(organicImpression)
      .setOrganicClickPerImpression(organicClickPerImpression)
      .setAdClickPerImpression(adClickPerImpression)
      .setOrganicDepthRatio(organicImpression.toFloat/20)
      .setAdClick(adClick)
      .setAdImpression(adImpression)
      .setAdDepthRatio(adImpression.toFloat/8)
      .setAdClickRatio(adClickRatio)
      .setTime(time)
      .build()
    out.collect(jseTrackingRequestKpi)
  }
}

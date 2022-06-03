
package inc.stanby.windows

import inc.stanby.schema.{JseTrackingSearchKpi, JseTracker}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcJseSearchKpiWindowFunction extends ProcessWindowFunction[JseTracker, JseTrackingSearchKpi, String, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");

  override def process(key: String, context: ProcessWindowFunction[JseTracker, JseTrackingSearchKpi, String, TimeWindow]#Context, input: lang.Iterable[JseTracker], out: Collector[JseTrackingSearchKpi]): Unit = {
    logger.info("Calc JSE Search Kpi Process Function been initialized")
    val inputList = input.asScala
    var eventCount = 0
    var jobImpressionCount = 0
    var jobClickCount = 0
    var adImpressionCount = 0
    var adClickCount = 0
    var relatedJobsClickCount = 0
    var jobDetailsImpressionCount = 0
    var organicAdClickRatio = 0
    var adClickRatio = 0
    var jobImpressionPerClick = 0.0
    var adImpressionPerClick = 0.0
    val time = inputList.head.getTime
    for (in <- inputList) {
      println(in.toString)
      if (in.getEventType.toString.equals("jobImpression") && !in.getIsAd) {
        jobImpressionCount += 1
      }
      if (in.getEventType.toString.equals("jobClick") && !in.getIsAd) {
        jobClickCount += 1
      }
      if (in.getEventType.toString.equals("jobImpression") && in.getIsAd) {
        adImpressionCount += 1
      }
      if (in.getEventType.toString.equals("jobClick") && in.getIsAd) {
        adClickCount += 1
      }
      if (in.getEventType.toString.equals("relatedJobsClick")) {
        relatedJobsClickCount += 1
      }
      if (in.getEventType.toString.equals("jobDetailsImpression")) {
        jobDetailsImpressionCount += 1
      }
      eventCount += 1
    }

    if (jobClickCount > 0 && adClickCount > 0) {
      adClickRatio = adClickCount/(jobClickCount+adClickCount)
      organicAdClickRatio = jobClickCount/adClickCount
    }

    if (jobImpressionCount > 0) {
      jobImpressionPerClick = jobClickCount.toFloat / jobImpressionCount
    }

    if (adImpressionCount > 0) {
      adImpressionPerClick = adClickCount.toFloat / adImpressionCount
    }

    val jseTrackingSearchKpi = JseTrackingSearchKpi.newBuilder
      .setSearchRequestId(key)
      .setEventCount(eventCount)
      .setJobClickCount(jobClickCount)
      .setJobDetailsImpressionCount(jobDetailsImpressionCount)
      .setRelatedJobsClickCount(relatedJobsClickCount)
      .setJobImpressionCount(jobImpressionCount)
      .setJobImpressionPerClick(jobImpressionPerClick)
      .setAdImpressionPerClick(adImpressionPerClick)
      .setJobImpressionCoverage(jobImpressionCount.toFloat/20)
      .setAdClickCount(adClickCount)
      .setAdImpressionCount(adImpressionCount)
      .setAdImpressionCoverage(adImpressionCount.toFloat/8)
      .setOrganicAdClickRatio(organicAdClickRatio)
      .setAdClickRatio(adClickRatio)
      .setTime(time)
      .build()
    out.collect(jseTrackingSearchKpi)
  }
}

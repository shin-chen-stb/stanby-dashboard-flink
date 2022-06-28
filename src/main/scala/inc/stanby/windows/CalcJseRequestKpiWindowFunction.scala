
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
    var jobRequestMatched = 0
    var jobImpression = 0
    var jobCT = 0
    var jobCTPerJobImpression = 0.0
    var adCTRatio = 0.0
    var organicRequestMatched = 0
    var organicImpression = 0
    var organicCT = 0
    var organicCTPerImpression = 0.0
    var adRequestMatched = 0
    var adImpression = 0
    var adCT = 0
    var adCTPerImpression = 0.0
    var origin = "other"
    val time = inputList.head.getTime
    for (in <- inputList) {
      println(in.toString)
      if (in.getEventType.toString.equals("jobImpression") && !in.getIsAd) {
        organicImpression += 1
      }
      if (in.getEventType.toString.equals("jobClick") && !in.getIsAd) {
        organicCT += 1
      }
      if (in.getEventType.toString.equals("jobImpression") && in.getIsAd) {
        adImpression += 1
      }
      if (in.getEventType.toString.equals("jobClick") && in.getIsAd) {
        adCT += 1
      }
      eventCount += 1
    }
    jobImpression = adImpression + organicImpression
    jobCT = adCT + organicCT

    if (jobCT > 0) {
      adCTRatio = adCT / jobCT
    }

    if (jobImpression > 0) {
      jobCTPerJobImpression = jobCT.toFloat / jobImpression
    }

    if (organicImpression > 0) {
      organicCTPerImpression = organicCT.toFloat / organicImpression
    }

    if (adImpression > 0) {
      adCTPerImpression = adCT.toFloat / adImpression
    }

    if (jobImpression > 0) {
      jobRequestMatched += 1
    }
    if (organicImpression > 0) {
      organicRequestMatched += 1
    }
    if (adImpression > 0) {
      adRequestMatched += 1
    }

    val jseTrackingRequestKpi = JseTrackingRequestKpi.newBuilder
      .setSearchRequestId(key)
      .setJobRequest(1)
      .setJobRequestMatched(jobRequestMatched)
      .setJobImpression(jobImpression)
      .setJobCT(jobCT)
      .setJobICTR(jobCTPerJobImpression)
      .setOrganicRequest(1)
      .setOrganicRequestMatched(organicRequestMatched)
      .setOrganicImpression(organicImpression)
      .setOrganicCT(organicCT)
      .setOrganicICTR(organicCTPerImpression)
      .setAdRequest(1)
      .setAdRequestMatched(adRequestMatched)
      .setAdImpression(adImpression)
      .setAdCT(adCT)
      .setAdICTR(adCTPerImpression)
      .setTime(time)
      .build()
    out.collect(jseTrackingRequestKpi)
  }
}

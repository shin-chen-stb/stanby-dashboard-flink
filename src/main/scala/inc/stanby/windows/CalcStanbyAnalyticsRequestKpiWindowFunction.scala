package inc.stanby.windows

import inc.stanby.schema.{StanbyEvent, StanbyAnalyticsRequestKpi}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcStanbyAnalyticsRequestKpiWindowFunction extends ProcessWindowFunction[StanbyEvent, StanbyAnalyticsRequestKpi, String, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");

  override def process(key: String, context: ProcessWindowFunction[StanbyEvent, StanbyAnalyticsRequestKpi, String, TimeWindow]#Context, input: lang.Iterable[StanbyEvent], out: Collector[StanbyAnalyticsRequestKpi]): Unit = {
    logger.info("Calc Search Kpi Process Function been initialized")
    val inputList = input.asScala
    var eventCount = 0
    var jobRequestMatched = 0
    var jobVImpression = 0
    var jobCT = 0
    var jobCTPerJobVImpression = 0.0
    var organicRequestMatched = 0
    var organicVImpression = 0
    var organicCT = 0
    var organicCTPerVImpression = 0.0
    var adRequestMatched = 0
    var adVImpression = 0
    var adCT = 0
    var adCTPerVImpression = 0.0
    var origin = "other"
    for (in <- inputList) {
      if (eventCount == 0 && !(in.getCurrentUrl == null && in.getFromYahoo == null)) {
        if (in.getFromYahoo) {
          origin = "yahoo"
        }
        else if (in.getCurrentUrl.toString.startsWith("/r_")) {
          origin = "rhash"
        }
      }
      if (in.getArea != null && in.getElement != null && in.getEventType != null && in.getPage != null) {
        jobCT += 1
        if (in.getEventType.toString.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card")) {
          if (in.getElement.toString.equals("求人")) {
            organicCT += 1
          }
          if (in.getElement.toString.equals("広告")) {
            adCT += 1
          }
        }
        if (in.getPage.toString.equals("search") && in.getEventType.toString.equals("viewable") && in.getArea.toString.equals("card")) {
          jobVImpression += 1
          if (in.getElement.toString.equals("求人")) {
            organicVImpression += 1
          }
          if (in.getElement.toString.equals("広告")) {
            adVImpression += 1
          }
        }
      }
      eventCount += 1
    }

    if (jobVImpression > 0) {
      jobCTPerJobVImpression = jobCT.toFloat / jobVImpression
    }

    if (organicVImpression > 0) {
      organicCTPerVImpression = organicCT.toFloat / organicVImpression
    }

    if (adVImpression > 0) {
      adCTPerVImpression = adCT.toFloat / adVImpression
    }

    if (jobVImpression > 0) {
      jobRequestMatched += 1
    }
    if (organicVImpression > 0) {
      organicRequestMatched += 1
    }
    if (adVImpression > 0) {
      adRequestMatched += 1
    }
    val now = new Date(inputList.head.getEpoch)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val time = dateFormat.format(now)

    val stanbyEventSearchKpi = StanbyAnalyticsRequestKpi.newBuilder
      .setSearchRequestId(inputList.head.getSearchRequestId.toString)
      .setJobRequest(1)
      .setJobRequestMatched(jobRequestMatched)
      .setJobVImpression(jobVImpression)
      .setJobCT(jobCT)
      .setJobCTPerVImpression(jobCTPerJobVImpression)
      .setOrganicRequest(1)
      .setOrganicRequestMatched(organicRequestMatched)
      .setOrganicVImpression(organicVImpression)
      .setOrganicCT(organicCT)
      .setOrganicCTPerVImpression(organicCTPerVImpression)
      .setAdRequest(1)
      .setAdRequestMatched(adRequestMatched)
      .setAdVImpression(adVImpression)
      .setAdCT(adCT)
      .setAdCTPerVImpression(adCTPerVImpression)
      .setOrigin(origin)
      .setSearchRequestId(key)
      .setTime(time)
      .build()
    out.collect(stanbyEventSearchKpi)
  }
}

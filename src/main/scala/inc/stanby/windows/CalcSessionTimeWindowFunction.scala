package inc.stanby.windows

import inc.stanby.schema.{StanbyEvent, StanbyEventSession}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcSessionTimeWindowFunction extends ProcessWindowFunction[StanbyEvent, StanbyEventSession, String, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");

  override def process(key: String, context: ProcessWindowFunction[StanbyEvent, StanbyEventSession, String, TimeWindow]#Context, input: lang.Iterable[StanbyEvent], out: Collector[StanbyEventSession]): Unit = {
    logger.info("CalcSession Process Function been initialized")
    var maxEpoch = 0L
    var minEpoch = Long.MaxValue
    val inputList = input.asScala
    var eventCount = 0
    var jobSearchCount = 0
    var jobDetailCount = 0
    var jobViewableCount = 0
    var jobCTCount = 0
    var adDetailCount = 0
    var adCTCount = 0
    var adViewableCount = 0
    var applyJobCount = 0
    var adCTPerSearch = 0.0
    var jobCTPerSearch = 0.0
    var adViewablePerSearch = 0.0
    var jobVieablePerSearch = 0.0
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
      if (in.getEventType != null && in.getPage != null) {
        if (in.getEventType.toString.equals("transition") && in.getPage.toString.equals("search")) {
          jobSearchCount += 1
        }
      }
      if (in.getArea != null && in.getElement != null && in.getEventType != null && in.getPage != null) {
        if (in.getEventType.toString.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card") && in.getElement.toString.equals("??????")) {
          jobCTCount += 1
        }
        if (in.getEventType.toString.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card") && in.getElement.toString.equals("??????")) {
          adCTCount += 1
        }
        if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("card") && in.getElement.toString.equals("??????")) {
          jobDetailCount += 1
        }
        if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("card") && in.getElement.toString.equals("??????")) {
          adDetailCount += 1
        }
        if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("content") && in.getElement.toString.equals("???????????????")) {
          applyJobCount += 1
        }
        if (in.getPage.toString.equals("search") && in.getEventType.toString.equals("viewable") && in.getArea.toString.equals("card") && in.getElement.toString.equals("??????")) {
          jobViewableCount += 1
        }
        if (in.getPage.toString.equals("search") && in.getEventType.toString.equals("viewable") && in.getArea.toString.equals("card") && in.getElement.toString.equals("??????")) {
          adViewableCount += 1
        }
        if (jobSearchCount > 0) {
          jobCTPerSearch = jobCTCount / jobSearchCount
          jobVieablePerSearch = jobViewableCount / jobSearchCount
          adCTPerSearch = adCTCount / jobSearchCount
          adViewablePerSearch = adViewableCount / jobSearchCount
        }
      }
      minEpoch = math.min(minEpoch, in.getEpoch)
      maxEpoch = math.max(maxEpoch, in.getEpoch)
      eventCount += 1
    }

    val d = new Date(maxEpoch)
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val sessionEndTime = dateFormat.format(d)
    val time = dateFormat.format(now)
    val res = (maxEpoch - minEpoch) / 1000

    val sessionEvent = StanbyEventSession.newBuilder
      .setSessionTime(res)
      .setEventCount(eventCount)
      .setJobSearchCount(jobSearchCount)
      .setJobDetailCount(jobDetailCount)
      .setJobCTCount(jobCTCount)
      .setJobViewableCount(jobViewableCount)
      .setAdDetailCount(adDetailCount)
      .setAdCTCount(adCTCount)
      .setAdViewableCount(adViewableCount)
      .setAdViewablePerSearch(adViewablePerSearch)
      .setJobViewablePerSearch(jobVieablePerSearch)
      .setAdCTPerSearch(adCTPerSearch)
      .setJobCTPerSearch(jobCTPerSearch)
      .setApplyJobCount(applyJobCount)
      .setOrigin(origin)
      .setSessionEndTime(sessionEndTime)
      .setSsid(key)
      .setTime(time)
      .build()
    out.collect(sessionEvent)
  }
}

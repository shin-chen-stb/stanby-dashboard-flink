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
    var adDetailCount = 0
    var applyJobCount = 0
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
      if (in.getEventType != null && in.getPage != null && in.getArea != null && in.getEventType.toString.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card")) {
        jobSearchCount += 1
      }
      if (in.getArea != null && in.getElement != null && in.getElement != null) {
        if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("card") && in.getElement.toString.equals("求人")) {
          jobDetailCount += 1
        }
        if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("card") && in.getElement.toString.equals("広告")) {
          adDetailCount += 1
        }
        if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("content") && in.getElement.toString.equals("応募ボタン")) {
          applyJobCount += 1
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
      .setAdDetailCount(adDetailCount)
      .setApplyJobCount(applyJobCount)
      .setOrigin(origin)
      .setSessionEndTime(sessionEndTime)
      .setSsid(key)
      .setTime(time)
      .build()
    out.collect(sessionEvent)
  }
}

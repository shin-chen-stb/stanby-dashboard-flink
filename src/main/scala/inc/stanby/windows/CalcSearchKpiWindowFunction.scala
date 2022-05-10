package inc.stanby.windows

import inc.stanby.schema.{StanbyEvent, StanbyEventSearchKpi, StanbyEventSession}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcSearchKpiWindowFunction extends ProcessWindowFunction[StanbyEvent, StanbyEventSearchKpi, String, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");

  override def process(key: String, context: ProcessWindowFunction[StanbyEvent, StanbyEventSearchKpi, String, TimeWindow]#Context, input: lang.Iterable[StanbyEvent], out: Collector[StanbyEventSearchKpi]): Unit = {
    logger.info("Calc Search Kpi Process Function been initialized")
    val inputList = input.asScala
    var eventCount = 0
    var jobViewableCount = 0
    var jobClickCount = 0
    var adClickCount = 0
    var adViewableCount = 0
    var searchPageCount = 0
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
          searchPageCount += 1
        }
      }
      if (in.getArea != null && in.getElement != null && in.getEventType != null && in.getPage != null) {
        if (in.getEventType.toString.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card") && in.getElement.toString.equals("求人")) {
          jobClickCount += 1
        }
        if (in.getEventType.toString.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card") && in.getElement.toString.equals("広告")) {
          adClickCount += 1
        }
        if (in.getPage.toString.equals("search") && in.getEventType.toString.equals("viewable") && in.getArea.toString.equals("card") && in.getElement.toString.equals("求人")) {
          jobViewableCount += 1
        }
        if (in.getPage.toString.equals("search") && in.getEventType.toString.equals("viewable") && in.getArea.toString.equals("card") && in.getElement.toString.equals("広告")) {
          adViewableCount += 1
        }
      }
      eventCount += 1
    }

    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val time = dateFormat.format(now)

    val stanbyEventSearchKpi = StanbyEventSearchKpi.newBuilder
      .setEventCount(eventCount)
      .setJobClickCount(jobClickCount)
      .setJobViewableCount(jobViewableCount)
      .setAdClickCount(adClickCount)
      .setAdViewableCount(adViewableCount)
      .setOrigin(origin)
      .setSsid(key)
      .setTime(time)
      .build()
    out.collect(stanbyEventSearchKpi)
  }
}

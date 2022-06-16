package inc.stanby.windows

import inc.stanby.schema.{StanbyEventSearchKpi, StanbyEventSearchKpiAgg}
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcMatchedStanbyEventKpiWindowFunction extends ProcessAllWindowFunction[StanbyEventSearchKpi, StanbyEventSearchKpiAgg, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcKpiTumblingTimeWindowFunction");

  override def process(context: ProcessAllWindowFunction[StanbyEventSearchKpi, StanbyEventSearchKpiAgg, TimeWindow]#Context, input: lang.Iterable[StanbyEventSearchKpi], out: Collector[StanbyEventSearchKpiAgg]): Unit = {
    logger.info("Calc Search Kpi Tumbling Window Function been initialized")
    val inputList = input.asScala
    var jobSearchCount = 0
    var matchedJobSearchCount = 0
    var jobViewableCount = 0
    var jobClickCount = 0
    var adClickCount = 0
    var adViewableCount = 0
    var biddedAdSearchCount = 0
    var adClickPerViewable = 0.0
    var jobViewablePerClick = 0.0
    var jobClickPerViewable = 0.0
    var adViewablePerClick = 0.0
    var matchedJobClickPerViewable = 0.0
    var jobClickPerSearch = 0.0
    var adClickPerSearch = 0.0
    var matchedJobClickPerSearch = 0.0
    var biddedAdClickPerSearch = 0.0
    var matchedOrganicRate = 0.0
    var biddedAdRate = 0.0
    for (in <- inputList) {
      jobSearchCount += 1
      if (in.getJobViewableCount > 0) {
        matchedJobSearchCount += 1
      }
      if (in.getAdViewableCount > 0) {
        biddedAdSearchCount += 1
      }
      jobViewableCount += in.getJobViewableCount
      jobClickCount = in.getJobClickCount
      adViewableCount += in.getAdViewableCount
      adClickCount += in.getAdClickCount
    }

    if (jobViewableCount > 0) {
      jobClickPerViewable = jobClickCount.toFloat / jobViewableCount
      matchedJobClickPerViewable = jobClickCount.toFloat / jobViewableCount
    }

    if (adViewableCount > 0) {
      adClickPerViewable = adClickCount.toFloat / adViewableCount
    }

    if (jobClickCount > 0) {
      jobViewablePerClick = jobViewableCount.toFloat / jobClickCount
      matchedOrganicRate = matchedJobSearchCount.toFloat / jobSearchCount
    }

    if (adClickCount > 0) {
      adViewablePerClick = adViewableCount.toFloat / adClickCount
      biddedAdRate = biddedAdSearchCount.toFloat / adClickCount
    }

    if (jobSearchCount > 0) {
      jobClickPerSearch = jobClickCount.toFloat / jobSearchCount
      adClickPerSearch = adClickCount.toFloat / jobSearchCount
    }

    if (matchedJobSearchCount > 0) {
      matchedJobClickPerSearch = jobClickCount.toFloat / matchedJobSearchCount
    }
    if (biddedAdSearchCount > 0) {
      biddedAdClickPerSearch = adClickCount.toFloat / biddedAdSearchCount
    }
    val time = inputList.head.getTime

    val jseMatchedSearchKpi = StanbyEventSearchKpiAgg.newBuilder
      .setJobClickCount(jobClickCount)
      .setAdClickCount(adClickCount)
      .setJobViewableCount(jobViewableCount)
      .setAdViewableCount(adViewableCount)
      .setJobClickPerViewable(jobClickPerViewable)
      .setAdClickPerViewable(adClickPerViewable)
      .setJobClickPerSearch(jobClickPerSearch)
      .setAdClickPerSearch(adClickPerSearch)
      .setMatchedJobSearchCount(matchedJobSearchCount)
      .setBiddedAdSearchCount(biddedAdSearchCount)
      .setMatchedOrganicRate(matchedOrganicRate)
      .setBiddedAdRate(biddedAdRate)
      .setBiddedAdClickPerSearch(biddedAdClickPerSearch)
      .setMatchedjobClickPerSearch(matchedJobClickPerSearch)
      .setTime(time)
      .build()
    out.collect(jseMatchedSearchKpi)
  }
}

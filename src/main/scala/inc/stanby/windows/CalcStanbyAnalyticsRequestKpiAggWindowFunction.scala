package inc.stanby.windows

import inc.stanby.schema.{StanbyAnalyticsRequestKpi, StanbyAnalyticsRequestKpiAgg, StanbyEventSearchKpi, StanbyEventSearchKpiAgg}
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcStanbyAnalyticsRequestKpiAggWindowFunction extends ProcessAllWindowFunction[StanbyAnalyticsRequestKpi, StanbyAnalyticsRequestKpiAgg, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcKpiTumblingTimeWindowFunction");

  override def process(context: ProcessAllWindowFunction[StanbyAnalyticsRequestKpi, StanbyAnalyticsRequestKpiAgg, TimeWindow]#Context, input: lang.Iterable[StanbyAnalyticsRequestKpi], out: Collector[StanbyAnalyticsRequestKpiAgg]): Unit = {
    logger.info("Calc Search Kpi Tumbling Window Function been initialized")
    val inputList = input.asScala
    var jobSearchCount = 0
    var matchedJobSearchCount = 0
    var organicViewableImpression = 0
    var organicCT = 0
    var adCT = 0
    var adViewableCount = 0
    var biddedAdSearchCount = 0
    var adCTPerViewable = 0.0
    var jobViewablePerCT = 0.0
    var jobCTPerViewable = 0.0
    var adViewablePerCT = 0.0
    var matchedJobCTPerViewable = 0.0
    var jobCTPerSearch = 0.0
    var adCTPerSearch = 0.0
    var matchedJobCTPerSearch = 0.0
    var biddedAdCTPerSearch = 0.0
    var matchedOrganicRate = 0.0
    var biddedAdRate = 0.0
    for (in <- inputList) {
      jobSearchCount += 1
      if (in.getorganicViewableImpression > 0) {
        matchedJobSearchCount += 1
      }
      if (in.getAdViewableCount > 0) {
        biddedAdSearchCount += 1
      }
      organicViewableImpression += in.getorganicViewableImpression
      organicCT += in.getorganicClick
      adViewableCount += in.getAdViewableCount
      adClick += in.getadClick
    }

    if (organicViewableImpression > 0) {
      jobClickPerViewable = organicClick.toFloat / organicViewableImpression
      matchedJobClickPerViewable = organicClick.toFloat / organicViewableImpression
    }

    if (adViewableCount > 0) {
      adClickPerViewable = adClick.toFloat / adViewableCount
    }

    if (organicClick > 0) {
      jobViewablePerClick = organicViewableImpression.toFloat / organicClick
      matchedOrganicRate = matchedJobSearchCount.toFloat / jobSearchCount
    }

    if (adClick > 0) {
      adViewablePerClick = adViewableCount.toFloat / adClick
      biddedAdRate = biddedAdSearchCount.toFloat / adClick
    }

    if (jobSearchCount > 0) {
      jobClickPerSearch = organicClick.toFloat / jobSearchCount
      adClickPerSearch = adClick.toFloat / jobSearchCount
    }

    if (matchedJobSearchCount > 0) {
      matchedJobClickPerSearch = organicClick.toFloat / matchedJobSearchCount
    }
    if (biddedAdSearchCount > 0) {
      biddedAdClickPerSearch = adClick.toFloat / biddedAdSearchCount
    }
    val time = inputList.head.getTime

    val stanbyAnalyticsRequestKpiAgg = StanbyAnalyticsRequestKpiAgg.newBuilder
      .setOrganicClick(organicClick)
      .setAdClick(adClick)
      .setOrganicViewableImpression(organicViewableImpression)
      .setAdViewableImpression(adViewableCount)
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
    out.collect(stanbyAnalyticsRequestKpiAgg)
  }
}

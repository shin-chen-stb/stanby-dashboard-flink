package inc.stanby.windows

import inc.stanby.schema.{JseTrackingSearchKpi, JseMatchedSearchKpi}
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcMatchedJseKpiWindowFunction extends ProcessAllWindowFunction[JseTrackingSearchKpi, JseMatchedSearchKpi, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcKpiTumblingTimeWindowFunction");

  override def process(context: ProcessAllWindowFunction[JseTrackingSearchKpi, JseMatchedSearchKpi, TimeWindow]#Context, input: lang.Iterable[JseTrackingSearchKpi], out: Collector[JseMatchedSearchKpi]): Unit = {
    logger.info("Calc Search Kpi Tumbling Window Function been initialized")
    val inputList = input.asScala
    var jobSearchCount = 0
    var matchedJobSearchCount = 0
    var jobImpressionCount = 0
    var jobClickCount = 0
    var adClickCount = 0
    var adImpressionCount = 0
    var biddedAdSearchCount = 0
    var adClickPerImpression = 0.0
    var jobImpressionPerClick = 0.0
    var jobClickPerImpression = 0.0
    var adImpressionPerClick = 0.0
    var adBiddedClickPerImpression = 0.0
    var matchedJobClickPerImpression = 0.0
    var jobClickPerSearch = 0.0
    var adClickPerSearch = 0.0
    var matchedJobClickPerSearch = 0.0
    var biddedAdClickPerSearch = 0.0
    var matchedOrganicRate = 0.0
    var biddedAdRate = 0.0
    for (in <- inputList) {
      jobSearchCount += 1
      if (in.getJobImpressionCount > 0) {
        matchedJobSearchCount += 1
      }
      if (in.getAdImpressionCount > 0) {
        biddedAdSearchCount += 1
      }
      jobImpressionCount += in.getJobImpressionCount
      jobClickCount = in.getJobClickCount
      adImpressionCount += in.getAdImpressionCount
      adClickCount += in.getAdClickCount
    }

    if (jobImpressionCount > 0) {
      jobClickPerImpression = jobClickCount.toFloat / jobImpressionCount
      matchedJobClickPerImpression = jobClickCount.toFloat / jobImpressionCount
    }

    if (adImpressionCount > 0) {
      adClickPerImpression = adClickCount.toFloat / adImpressionCount
      adBiddedClickPerImpression = adClickCount.toFloat / biddedAdSearchCount
    }

    if (jobClickCount > 0) {
      jobImpressionPerClick = jobImpressionCount.toFloat / jobClickCount
      matchedOrganicRate = matchedJobSearchCount.toFloat / jobSearchCount
    }

    if (adClickCount > 0) {
      adImpressionPerClick = adImpressionCount.toFloat / adClickCount
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

    val jseMatchedSearchKpi = JseMatchedSearchKpi.newBuilder
      .setJobClickCount(jobClickCount)
      .setAdClickCount(adClickCount)
      .setJobImpressionCount(jobImpressionCount)
      .setAdImpressionCount(adImpressionCount)
      .setJobClickPerImpression(jobClickPerImpression)
      .setAdClickPerImpression(adClickPerImpression)
      .setJobClickPerSearch(jobClickPerSearch)
      .setAdClickPerSearch(adClickPerSearch)
      .setMatchedJobSearchCount(matchedJobSearchCount)
      .setBiddedAdSearchCount(biddedAdSearchCount)
      .setMatchedOrganicRate(matchedOrganicRate)
      .setBiddedAdRate(biddedAdRate)
      .setTime(time)
      .build()
    out.collect(jseMatchedSearchKpi)
  }
}

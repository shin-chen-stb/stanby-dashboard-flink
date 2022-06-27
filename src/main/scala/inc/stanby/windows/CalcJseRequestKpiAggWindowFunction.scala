package inc.stanby.windows

import inc.stanby.schema.{JseTrackingRequestKpi, JseTrackingRequestKpiAgg}
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class CalcJseRequestKpiAggWindowFunction extends ProcessAllWindowFunction[JseTrackingRequestKpi, JseTrackingRequestKpiAgg, TimeWindow] {
  val logger: Logger = LoggerFactory.getLogger("CalcKpiTumblingTimeWindowFunction");

  override def process(context: ProcessAllWindowFunction[JseTrackingRequestKpi, JseTrackingRequestKpiAgg, TimeWindow]#Context, input: lang.Iterable[JseTrackingRequestKpi], out: Collector[JseTrackingRequestKpiAgg]): Unit = {
    logger.info("Calc Search Kpi Tumbling Window Function been initialized")
    val inputList = input.asScala
    var jobSearchCount = 0
    var matchedOrganicRequest = 0
    var organicImpression = 0
    var organicCT = 0
    var adCT = 0
    var adImpression = 0
    var matchedAdRequest = 0
    var adCTPerImpression = 0.0
    var jobImpressionPerCT = 0.0
    var organicCTPerImpression = 0.0
    var adImpressionPerCT = 0.0
    var adBiddedCTPerImpression = 0.0
    var matchedorganicCTPerImpression = 0.0
    var organicCTPerSearch = 0.0
    var adClickPerRequest = 0.0
    var matchedorganicClickPerSearch = 0.0
    var biddedadClickPerRequest = 0.0
    var matchedOrganicRate = 0.0
    var biddedAdRate = 0.0
    for (in <- inputList) {
      jobSearchCount += 1
      if (in.getOrganicImpression > 0) {
        matchedOrganicRequest += 1
      }
      if (in.getAdImpression > 0) {
        matchedAdRequest += 1
      }
      organicImpression += in.getOrganicImpression
      organicClick = in.getOrganicClick
      adImpression += in.getAdImpression
      adClick += in.getAdClick
    }

    if (organicImpression > 0) {
      organicClickPerImpression = organicClick.toFloat / organicImpression
      matchedorganicClickPerImpression = organicClick.toFloat / organicImpression
    }

    if (adImpression > 0) {
      adClickPerImpression = adClick.toFloat / adImpression
      adBiddedClickPerImpression = adClick.toFloat / matchedAdRequest
    }

    if (organicClick > 0) {
      jobImpressionPerClick = organicImpression.toFloat / organicClick
      matchedOrganicRate = matchedOrganicRequest.toFloat / jobSearchCount
    }

    if (adClick > 0) {
      adImpressionPerClick = adImpression.toFloat / adClick
      biddedAdRate = matchedAdRequest.toFloat / adClick
    }

    if (jobSearchCount > 0) {
      organicClickPerSearch = organicClick.toFloat / jobSearchCount
      adClickPerRequest = adClick.toFloat / jobSearchCount
    }

    if (matchedOrganicRequest > 0) {
      matchedorganicClickPerSearch = organicClick.toFloat / matchedOrganicRequest
    }
    if (matchedAdRequest > 0) {
      biddedadClickPerRequest = adClick.toFloat / matchedAdRequest
    }
    val time = inputList.head.getTime

    val jseTrackingRequestKpiAgg = JseTrackingRequestKpiAgg.newBuilder
      .setOrganicClick(organicClick)
      .setAdClick(adClick)
      .setOrganicImpression(organicImpression)
      .setAdImpression(adImpression)
      .setOrganicClickPerImpression(organicClickPerImpression)
      .setAdClickPerImpression(adClickPerImpression)
      .setOrganicClickPerRequest(organicClickPerSearch)
      .setAdClickPerRequest(adClickPerRequest)
      .setMatchedOrganicRequest(matchedOrganicRequest)
      .setMatchedAdRequest(matchedAdRequest)
      .setMatchedOrganicRate(matchedOrganicRate)
      .setMatchedAdRate(biddedAdRate)
      .setTime(time)
      .build()
    out.collect(jseTrackingRequestKpiAgg)
  }
}

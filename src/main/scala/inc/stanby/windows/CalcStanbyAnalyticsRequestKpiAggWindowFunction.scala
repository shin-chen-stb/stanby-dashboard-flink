package inc.stanby.windows

import inc.stanby.schema.{StanbyAnalyticsRequestKpi, StanbyAnalyticsRequestKpiAgg, StanbyAnalyticsRequestKpiAgg}
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
    var jobRequest = 0
    var jobRequestMatched = 0
    var jobVImpression = 0
    var jobCoverage = 0.0
    var jobCT = 0
    var jobVDepth = 0.0
    var jobVDepthMatched = 0.0
    var jobCTPerJobVImpression = 0.0
    var jobCTYield = 0.0
    var jobCTYieldMatched = 0.0
    var adCTRatio = 0.0
    var organicRequest = 0
    var organicRequestMatched = 0
    var organicVImpression = 0
    var organicVDepth = 0.0
    var organicVDepthMatched = 0.0
    var organicCoverage = 0.0
    var organicCT = 0
    var organicCTPerImpression = 0.0
    var organicPageCTR = 0.0
    var organicPageCTRMatched = 0.0
    var adRequest = 0
    var adRequestMatched = 0
    var adCoverage = 0.0
    var adVImpression = 0
    var adVDepth = 0.0
    var adVDepthMatched = 0.0
    var adCT = 0
    var adCTPerVImpression = 0.0
    var adPageCTR = 0.0
    var adPageCTRMatched = 0.0
    for (in <- inputList) {
      jobRequest += in.getJobRequest
      organicRequest += in.getOrganicRequest
      adRequest += in.getAdRequest
      jobRequestMatched += in.getJobRequestMatched
      organicRequestMatched += in.getOrganicRequestMatched
      adRequestMatched += in.getAdRequestMatched
      jobVImpression += in.getJobVImpression
      jobCT += in.getJobCT
      organicVImpression += in.getOrganicVImpression
      organicCT += in.getOrganicCT
      adVImpression += in.getAdVImpression
      adCT += in.getAdCT
    }
    jobCoverage = jobRequestMatched.toFloat / jobRequest
    organicCoverage = organicRequestMatched.toFloat / organicRequest
    adCoverage = adRequestMatched.toFloat / adRequest
    jobVDepth = jobVImpression.toFloat / jobRequest
    organicVDepth = organicVImpression.toFloat / organicRequest
    adVDepth = adVImpression.toFloat / adRequest
    organicPageCTR = organicCT.toFloat / organicRequest
    adPageCTR = adCT.toFloat / adRequest

    if (jobVImpression > 0) {
      jobCTPerJobVImpression = organicCT.toFloat / organicVImpression
    }

    if (organicVImpression > 0) {
      organicCTPerImpression = organicCT.toFloat / organicVImpression
    }

    if (adVImpression > 0) {
      adCTPerVImpression = adCT.toFloat / adVImpression
    }

    if (jobRequest > 0) {
      jobCTYield = jobCT.toFloat / jobRequest
    }

    if (jobRequestMatched > 0) {
      jobCTYieldMatched = jobCT.toFloat / jobRequestMatched
      jobVDepthMatched = jobVImpression.toFloat / jobRequestMatched
    }
    if (organicRequestMatched > 0) {
      organicPageCTRMatched = organicCT.toFloat / organicRequestMatched
      organicVDepthMatched = organicVImpression.toFloat / organicRequestMatched
    }
    if (adRequestMatched > 0) {
      adPageCTRMatched = adCT.toFloat / adRequestMatched
      adVDepthMatched = adVDepthMatched.toFloat / adRequestMatched
    }
    if (jobCT > 0) {
      adCTRatio = adCT.toFloat / jobCT
    }

    val time = inputList.head.getTime

    val stanbyAnalyticsRequestKpiAgg = StanbyAnalyticsRequestKpiAgg.newBuilder
      .setJobRequest(jobRequest)
      .setJobRequestMatched(jobRequestMatched)
      .setJobVImpression(jobVImpression)
      .setJobVDepth(jobVDepth)
      .setJobVDepthMatched(jobVDepthMatched)
      .setJobCoverage(jobCoverage)
      .setJobCT(jobCT)
      .setJobCTPerVImpression(jobCTPerJobVImpression)
      .setJobCTYield(jobCTYield)
      .setJobCTYieldMatched(jobCTYieldMatched)
      .setAdCTRatio(adCTRatio)
      .setOrganicRequest(organicRequest)
      .setOrganicRequestMatched(organicRequestMatched)
      .setOrganicVImpression(organicVImpression)
      .setOrganicVDepth(organicVDepth)
      .setOrganicVDepthMatched(organicVDepthMatched)
      .setOrganicCoverage(organicCoverage)
      .setOrganicCT(organicCT)
      .setOrganicCTPerVImpression(organicCTPerImpression)
      .setOrganicPageCTR(organicPageCTR)
      .setOrganicPageCTRMatched(organicPageCTRMatched)
      .setAdRequest(adRequest)
      .setAdRequestMatched(adRequestMatched)
      .setAdVImpression(adVImpression)
      .setAdVDepth(adVDepth)
      .setAdVDepthMatched(adVDepthMatched)
      .setAdCoverage(adCoverage)
      .setAdCTPerVImpression(adCTPerVImpression)
      .setAdCT(adCT)
      .setAdPageCTR(adPageCTR)
      .setAdPageCTRMatched(adPageCTRMatched)
      .setTime(time)
      .build()
    out.collect(stanbyAnalyticsRequestKpiAgg)
  }
}

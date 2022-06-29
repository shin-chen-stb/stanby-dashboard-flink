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
    var jobRequest = 0
    var jobRequestMatched = 0
    var jobImpression = 0
    var jobCoverage = 0.0
    var jobCT = 0
    var jobICTR = 0.0
    var jobDepth = 0.0
    var jobDepthMatched = 0.0
    var jobCTYield = 0.0
    var jobCTYieldMatched = 0.0
    var adCTRatio = 0.0
    var organicRequest = 0
    var organicRequestMatched = 0
    var organicImpression = 0
    var organicDepth = 0.0
    var organicDepthMatched = 0.0
    var organicActualDepthRatio = 0.0
    var organicActualDepthRatioMatched = 0.0
    var organicCoverage = 0.0
    var organicCT = 0
    var organicICTR = 0.0
    var organicPageCTR = 0.0
    var organicPageCTRMatched = 0.0
    var adRequest = 0
    var adRequestMatched = 0
    var adCoverage = 0.0
    var adImpression = 0
    var adDepth = 0.0
    var adDepthMatched = 0.0
    var adDepthRatio = 0.0
    var adActualDepthRatio = 0.0
    var adActualDepthRatioMatched = 0.0
    var adCT = 0
    var adICTR = 0.0
    var adPageCTR = 0.0
    var adPageCTRMatched = 0.0
    for (in <- inputList) {
      jobRequest += in.getJobRequest
      organicRequest += in.getOrganicRequest
      adRequest += in.getAdRequest
      jobRequestMatched += in.getJobRequestMatched
      organicRequestMatched += in.getOrganicRequestMatched
      adRequestMatched += in.getAdRequestMatched
      jobImpression += in.getJobImpression
      organicImpression += in.getOrganicImpression
      adImpression += in.getAdImpression
      jobCT += in.getJobCT
      organicCT += in.getOrganicCT
      adCT += in.getAdCT
    }
    jobCTYield = jobCT.toFloat / jobRequest
    organicPageCTR = organicCT.toFloat / organicRequest
    adPageCTR = adCT.toFloat / adRequest
    jobCoverage = jobRequestMatched.toFloat / jobRequest
    organicCoverage = organicRequestMatched.toFloat / organicRequest
    adCoverage = adRequestMatched.toFloat / adRequest
    jobDepth = jobImpression.toFloat / jobRequest
    organicDepth = organicImpression.toFloat / organicRequest
    adDepth = adImpression.toFloat / adRequest
    organicActualDepthRatio = organicDepth.toFloat / 20
    organicActualDepthRatioMatched = organicDepthMatched.toFloat / 20
    adActualDepthRatio = adDepth.toFloat / 8
    adActualDepthRatioMatched = adDepthMatched.toFloat / 8
    if (jobImpression > 0) {
      jobICTR = jobCT.toFloat / jobImpression
    }

    if (organicImpression > 0) {
      organicICTR = organicCT.toFloat / organicImpression
    }

    if (adImpression > 0) {
      adICTR = adCT.toFloat / adImpression
    }

    if (jobRequestMatched > 0) {
      jobCTYieldMatched = jobCT.toFloat / jobRequestMatched
      jobDepthMatched = jobImpression.toFloat / jobRequestMatched
    }

    if (organicRequestMatched > 0) {
      organicPageCTRMatched = organicCT.toFloat / organicRequestMatched
      organicDepthMatched = organicImpression.toFloat / organicRequestMatched
    }

    if (adRequestMatched > 0) {
      adPageCTRMatched = adCT.toFloat / adRequestMatched
      adDepthMatched = adDepthMatched.toFloat / adRequestMatched
    }

    if (jobDepthMatched > 0) {
      adDepthRatio = adDepthMatched.toFloat / jobDepthMatched
    }

    if (jobCT > 0) {
      adCTRatio = adCT.toFloat / jobCT
    }
    val time = inputList.head.getTime

    val jseTrackingRequestKpiAgg = JseTrackingRequestKpiAgg.newBuilder
      .setJobRequest(jobRequest)
      .setJobRequestMatched(jobRequestMatched)
      .setJobImpression(jobImpression)
      .setJobDepth(jobDepth)
      .setJobDepthMatched(jobDepthMatched)
      .setJobCoverage(jobCoverage)
      .setJobCT(jobCT)
      .setJobICTR(jobICTR)
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
    out.collect(jseTrackingRequestKpiAgg)
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package inc.stanby

import inc.stanby.operators.AmazonElasticsearchSink
import inc.stanby.schema._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.common.functions.FilterFunction

import java.util.{Date, Properties}
import inc.stanby.serializers.{AdTrackingDeserializationSchema, JseTrackerDeserializationSchema, StanbyEventDeserializationSchema}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.{lang, util}
import java.text.SimpleDateFormat
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object StreamingJob {
  val serviceName = "es";
  val region = "ap-northeast-1";
  val domainEndpoint = "https://search-chen-stanby-analytics-dev-dqo5rvvb3udyugdkkrrzjr5gda.ap-northeast-1.es.amazonaws.com";
  val logger: Logger = LoggerFactory.getLogger("Stanby_Dashboard_Flink");
  val inputProperties = new Properties
  inputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
  inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON")

  private def createStanbyEventSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKinesisConsumer[StanbyEvent]("dmt-dataplatform-analytics-stream", new StanbyEventDeserializationSchema, inputProperties))
  }

  private def createJseTrackerSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKinesisConsumer[JseTracker]("dmt-jse-tracker", new JseTrackerDeserializationSchema, inputProperties))
  }

  private def createAdTrackerSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKinesisConsumer[AdTracking]("dmt-ad-tracking", new AdTrackingDeserializationSchema(), inputProperties))
  }


  @throws[Exception]
  def main(args: Array[String]): Unit = { // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val StanbyEventStream = createStanbyEventSourceFromStaticConfig(env)
    val adTrackingStream = createAdTrackerSourceFromStaticConfig(env)
    val jseTrackerStream = createJseTrackerSourceFromStaticConfig(env)

    //   ------------------------------ ADTracking ------------------------------

    adTrackingStream.filter(new FilterFunction[AdTracking]() {
      @throws[Exception]
      override def filter(value: AdTracking): Boolean = value.getLogType.equals("click")
    }).map { case x => adTracking2AdTrackingClick(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "ad-tracking-click", "_doc"))

    adTrackingStream.filter(new FilterFunction[AdTracking]() {
      @throws[Exception]
      override def filter(value: AdTracking): Boolean = value.getLogType.equals("request")
    }).map { case x => adTracking2AdTrackingReuqest(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "ad-tracking-request", "_doc"))

    adTrackingStream.filter(new FilterFunction[AdTracking]() {
      @throws[Exception]
      override def filter(value: AdTracking): Boolean = value.getLogType.equals("distribution")
    }).map { case x => adTracking2AdTrackingDistribution(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "ad-tracking-distribution", "_doc"))

    //   ------------------------------ StanbyAnalytics ------------------------------

    val SessionWindowStream = StanbyEventStream.keyBy(new KeySelector[StanbyEvent, String] {
      override def getKey(event: StanbyEvent): String = {
        logger.info("GETKEY Event: " + event.getSsid.toString)
        event.getSsid.toString
      }
    }).window(ProcessingTimeSessionWindows.withGap(Time.minutes(5)))
      .process(new CalcSessionTimeWindowFunction())
    SessionWindowStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_session", "_doc"))

    SessionWindowStream.filter(new FilterFunction[StanbyEventSession]() {
      @throws[Exception]
      override def filter(value: StanbyEventSession): Boolean =
        value.getFromYahoo
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_session_from_yahoo", "_doc"))

    SessionWindowStream.filter(new FilterFunction[StanbyEventSession]() {
      @throws[Exception]
      override def filter(value: StanbyEventSession): Boolean =
        value.getFromRhash
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_session_from_rhash", "_doc"))

    SessionWindowStream.filter(new FilterFunction[StanbyEventSession]() {
      @throws[Exception]
      override def filter(value: StanbyEventSession): Boolean =
        !value.getFromRhash && value.getFromYahoo
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_session_from_other", "_doc"))

    StanbyEventStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event2", "_doc"))
    StanbyEventStream.filter(new FilterFunction[StanbyEvent]() {
      @throws[Exception]
      override def filter(value: StanbyEvent): Boolean =
        value.getEventType.equals("link") && value.getPage.toString.equals("search") && value.getElement.toString.equals("広告") && value.getArea.toString.equals("card")
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_ad", "_doc"))

    StanbyEventStream.filter(new FilterFunction[StanbyEvent]() {
      @throws[Exception]
      override def filter(value: StanbyEvent): Boolean = value.getEventType.toString.equals("link") && value.getPage.toString.equals("search") && value.getElement.toString.equals("広告") && value.getArea.toString.equals("card") && value.getFromYahoo == true
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_ad_from_yahoo", "_doc"))

    StanbyEventStream.filter(new FilterFunction[StanbyEvent]() {
      @throws[Exception]
      override def filter(value: StanbyEvent): Boolean = value.getEventType.toString.equals("link") && value.getPage.toString.equals("search") && value.getElement.toString.equals("求人") && value.getArea.toString.equals("card") && value.getCurrentUrl.toString.startsWith("r_") && value.getFromYahoo == false
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_from_rhash", "_doc"))

    StanbyEventStream.filter(new FilterFunction[StanbyEvent]() {
      @throws[Exception]
      override def filter(value: StanbyEvent): Boolean = value.getEventType.toString.equals("link") && value.getPage.toString.equals("search") && value.getElement.toString.equals("求人") && value.getArea.toString.equals("card") && value.getFromYahoo == true
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_from_yahoo", "_doc"))

    StanbyEventStream.filter(new FilterFunction[StanbyEvent]() {
      @throws[Exception]
      override def filter(value: StanbyEvent): Boolean = value.getEventType.toString.equals("link") && value.getPage.toString.equals("search") && value.getElement.toString.equals("求人") && value.getArea.toString.equals("card") && !value.getCurrentUrl.toString.startsWith("r_") && value.getFromYahoo == false
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_from_other", "_doc"))

    //   ------------------------------ JseTracking ------------------------------
    //    input2.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-tracker", "_doc"))

    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobSearchRequest")
    }).map { case x => jseTracker2JseTrackingJobSearchRequest(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-search2", "_doc"))

    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobDetailsImpression")
    }).map { case x => jseTracker2JseTrackingJobDetailsImpression(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-detail-impression2", "_doc"))

    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobImpression")
    }).map { case x => jseTracker2JseTrackingJobImpression(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-impression2", "_doc"))

    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobClick")
    }).map { case x => jseTracker2JseTrackingJobClick(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-click2", "_doc"))

    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("relatedJobsClick")
    }).map { case x => jseTracker2JseTrackingRelatedJobClick(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-related-job-click2", "_doc"))

    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = (value.getGeoLocation != null && value.getGeoLocation.toString.nonEmpty)
    }).map { case x => jseTracker2JseTrackingJobSearchRequest(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-request-geolocation2", "_doc"))

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }


  def adTracking2AdTrackingReuqest(event: AdTracking): AdTrackingRequest = {
    AdTrackingRequest
      .newBuilder
      .setLogType(event.getLogType)
      .setBucketType(event.getBucketType)
      .setDeviceType(event.getDeviceType)
      .setCreateDateTime(event.getCreateDateTime)
      .setPublisherId(event.getPublisherId)
      .setIp(event.getIp)
      .setPublisherChannelId(event.getPublisherChannelId)
      .setPublisherChannelType(event.getPublisherChannelType)
      .setBucketType(event.getBucketType)
      .setSystemId(event.getSystemId)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId)
      .setSalaryUnit(event.getSalaryUnit)
      .setSalaryMax(event.getSalaryMax)
      .setSalaryMin(event.getSalaryMin)
      .setYahooCompanyId(event.getYahooCompanyId)
      .setLocation(event.getLocation)
      .setDistanceLat(event.getDistanceLat)
      .setDistanceLon(event.getDistanceLon)
      .setAlgorithm(event.getAlgorithm)
      .setIsBackfill(event.getIsBackfill)
      .setTotalHits(event.getTotalHits)
      .setAddressLevel(event.getAddressLevel)
      .setAddressLevel1(event.getAddressLevel1)
      .setAddressLevel2(event.getAddressLevel2)
      .setRequestDateTime(event.getRequestDateTime)
      .setLimit(event.getLimit)
      .setKeyword(event.getKeyword)
      .setOffset(event.getOffset)
      .setSort(event.getSort)
      .setPrefectureCode(event.getPrefectureCode)
      .setMode(event.getMode)
      .setNotKeyword(event.getNotKeyword)
      .build
  }

  def adTracking2AdTrackingDistribution(event: AdTracking): AdTrackingDistribution = {
    AdTrackingDistribution
      .newBuilder
      .setLogType(event.getLogType)
      .setDistributionId(event.getDistributionId)
      .setBucketType(event.getBucketType)
      .setRequestId(event.getRequestId)
      .setDeviceType(event.getDeviceType)
      .setCreateDateTime(event.getCreateDateTime)
      .setDocumentId(event.getDocumentId)
      .setPublisherId(event.getPublisherId)
      .setIp(event.getIp)
      .setPublisherChannelId(event.getPublisherChannelId)
      .setPublisherChannelType(event.getPublisherChannelType)
      .setBucketType(event.getBucketType)
      .setDocumentUrl(event.getDocumentUrl)
      .setSiteCode(event.getSiteCode)
      .setBidCpc(event.getBidCpc)
      .setDistributionOrder(event.getDistributionOrder)
      .setSystemId(event.getSystemId)
      .setTrackingParam(event.getTrackingParam)
      .setRemoveParam(event.getRemoveParam)
      .setAdvertiserId(event.getAdvertiserId)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId)
      .setCampaignId(event.getCampaignId)
      .setCampaignType(event.getCampaignType)
      .setMaxCpc(event.getMaxCpc)
      .setSalaryUnit(event.getSalaryUnit)
      .setSalaryMax(event.getSalaryMax)
      .setSalaryMin(event.getSalaryMin)
      .setJobChannelType(event.getJobChannelType)
      .setPredictCtr(event.getPredictCtr)
      .setMaxCpc(event.getMaxCpc)
      .build
  }

  def adTracking2AdTrackingClick(event: AdTracking): AdTrackingClick = {
    AdTrackingClick
      .newBuilder
      .setLogType(event.getLogType)
      .setDistributionId(event.getDistributionId)
      .setBucketType(event.getBucketType)
      .setCategory(event.getCategory)
      .setRequestId(event.getRequestId)
      .setDeviceType(event.getDeviceType)
      .setCreateDateTime(event.getCreateDateTime)
      .setDocumentId(event.getDocumentId)
      .setPublisherId(event.getPublisherId)
      .setIp(event.getIp)
      .setPublisherChannelId(event.getPublisherChannelId)
      .setPublisherChannelType(event.getPublisherChannelType)
      .setBucketType(event.getBucketType)
      .setDocumentUrl(event.getDocumentUrl)
      .setSiteCode(event.getSiteCode)
      .setLogHost(event.getLogHost)
      .setBidCpc(event.getBidCpc)
      .setRole(event.getRole)
      .setDistributionOrder(event.getDistributionOrder)
      .setSystemId(event.getSystemId)
      .setTrackingParam(event.getTrackingParam)
      .setTag(event.getTag)
      .setRemoveParam(event.getRemoveParam)
      .setAdvertiserId(event.getAdvertiserId)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId)
      .setCampaignId(event.getCampaignId)
      .setCampaignType(event.getCampaignType)
      .setMaxCpc(event.getMaxCpc)
      .setSalaryUnit(event.getSalaryUnit)
      .setSalaryMax(event.getSalaryMax)
      .setSalaryMin(event.getSalaryMin)
      .build
  }

  def jseTracker2JseTrackingJobClick(event: JseTracker): JseTrackingJobClick = {
    JseTrackingJobClick
      .newBuilder
      .setBucketType(event.getBucketType)
      .setCategory(event.getBucketType)
      .setClickId(event.getBucketType)
      .setCreateDateTime(event.getCreateDateTime)
      .setDocumentId(event.getDocumentId)
      .setEventType(event.getEventType)
      .setIp(event.getIp)
      .setIsAd(event.getIsAd)
      .setLogHost(event.getLogHost)
      .setRole(event.getRole)
      .setSearchRequestId(event.getSearchRequestId)
      .setTag(event.getTag)
      .setTime(event.getTime)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId).build
  }

  def jseTracker2JseTrackingJobDetailsImpression(event: JseTracker): JseTrackingJobDetailsImpression = {
    JseTrackingJobDetailsImpression
      .newBuilder
      .setAdDistributionId(event.getAdDistributionId)
      .setAdJobs(event.getAdJobs)
      .setBucketType(event.getBucketType)
      .setCategory(event.getCategory)
      .setCompanyName(event.getCompanyName)
      .setCreateDateTime(event.getCreateDateTime)
      .setDocumentId(event.getDocumentId)
      .setEventType(event.getEventType)
      .setIp(event.getEventType)
      .setIndexType(event.getIndexType)
      .setIsAd(event.getIsAd)
      .setJobContent(event.getJobContent)
      .setJobTitle(event.getJobTitle)
      .setJobType(event.getJobType)
      .setLogHost(event.getLogHost)
      .setNewJobs(event.getNewJobs)
      .setOpenDate(event.getOpenDate)
      .setRelatedJobs(event.getRelatedJobs)
      .setRole(event.getRole)
      .setSiteName(event.getSiteName)
      .setTag(event.getTag)
      .setTime(event.getTime)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId)
      .build
  }

  def jseTracker2JseTrackingJobImpression(event: JseTracker): JseTrackingJobImpression = {
    JseTrackingJobImpression
      .newBuilder
      .setAdDistributionId(event.getAdDistributionId)
      .setAdJobs(event.getAdJobs)
      .setBucketType(event.getBucketType)
      .setCategory(event.getCategory)
      .setCompanyName(event.getCompanyName)
      .setCreateDateTime(event.getCreateDateTime)
      .setDocumentId(event.getDocumentId)
      .setEventType(event.getEventType)
      .setIp(event.getIp)
      .setIndexType(event.getIndexType)
      .setIsAd(event.getIsAd)
      .setJobContent(event.getJobContent)
      .setJobTitle(event.getJobTitle)
      .setJobType(event.getJobType)
      .setJobVersionDate(event.getJobVersionDate)
      .setLogHost(event.getLogHost)
      .setNewJobs(event.getNewJobs)
      .setOpenDate(event.getOpenDate)
      .setOrder(event.getOrder)
      .setRelatedJobs(event.getRelatedJobs)
      .setRole(event.getRole)
      .setSearchRequestId(event.getSearchRequestId)
      .setSiteName(event.getSiteName)
      .setTag(event.getTag)
      .setTime(event.getTime)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setUpdateDate(event.getUpdateDate)
      .setVisitId(event.getVisitId)
      .build
  }

  def jseTracker2JseTrackingRelatedJobClick(event: JseTracker): JseTrackingRelatedJobClick = {
    JseTrackingRelatedJobClick
      .newBuilder
      .setBucketType(event.getBucketType)
      .setCategory(event.getCategory)
      .setClickId(event.getClickId)
      .setCreateDateTime(event.getCreateDateTime)
      .setDocumentId(event.getDocumentId)
      .setEventType(event.getEventType)
      .setIp(event.getIp)
      .setIsAd(event.getIsAd)
      .setLogHost(event.getLogHost)
      .setOrder(event.getOrder)
      .setOriginDocumentId(event.getOriginDocumentId)
      .setRole(event.getRole)
      .setTag(event.getTag)
      .setTime(event.getTime)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId)
      .build
  }

  def jseTracker2JseTrackingJobSearchRequest(event: JseTracker): JseTrackingJobSearchRequest = {
    JseTrackingJobSearchRequest
      .newBuilder
      .setBucketType(event.getBucketType)
      .setCategory(event.getCategory)
      .setCompanyName(event.getCompanyName)
      .setCreateDateTime(event.getCreateDateTime)
      .setEventType(event.getEventType)
      .setIp(event.getIp)
      .setJobType(event.getJobType)
      .setLocation(event.getLocation)
      .setLogHost(event.getLogHost)
      .setOpenDate(event.getOpenDate)
      .setOriginalKeyword(event.getOriginalKeyword)
      .setReferer(event.getReferer)
      .setRole(event.getRole)
      .setSearchPage(event.getSearchPage)
      .setSearchRequestId(event.getSearchRequestId)
      .setSearchRequestUrl(event.getSearchRequestUrl)
      .setSiteCodes(event.getSiteCodes)
      .setTag(event.getTag)
      .setTime(event.getTime)
      .setTotalHits(event.getTotalHits)
      .setUaCategory(event.getUaCategory)
      .setUaValue(event.getUaValue)
      .setUid(event.getUid)
      .setVisitId(event.getVisitId)
      .setSalaryUnit(event.getSalaryUnit)
      .setSalaryMax(event.getSalaryMax)
      .setSalaryMin(event.getSalaryMin)
      .build
  }
}

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
    var fromYahoo = false
    var fromRhash = false
    for (in <- inputList) {
      if (eventCount == 0) {
        fromYahoo = in.getFromYahoo
        fromRhash = in.getCurrentUrl.toString.startsWith("r_")
      }
      if (in.getEventType.equals("link") && in.getPage.toString.equals("search") && in.getArea.toString.equals("card")) {
        jobSearchCount += 1
      }
      if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("card") && in.getElement.toString.equals("求人")) {
        jobDetailCount += 1
      }
      if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("card") && in.getElement.toString.equals("広告")) {
        adDetailCount += 1
      }
      if (in.getPage.toString.equals("job_detail") && in.getArea.toString.equals("content") && in.getElement.toString.equals("応募ボタン")) {
        applyJobCount += 1
      }
      minEpoch = math.min(minEpoch, in.getEpoch)
      maxEpoch = math.max(maxEpoch, in.getEpoch)
      eventCount += 1
    }

    val d = new Date(maxEpoch)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val sessionEndTime = dateFormat.format(d)
    val res = (maxEpoch - minEpoch) / 1000
//    val o = "{\"Session\":\"%s\",\"SessionTime\":%d, \"epoch\":%d}".format(key, res, maxEpoch)
    val sessionEvent = StanbyEventSession.newBuilder
      .setSessionTime(res)
      .setEventCount(eventCount)
      .setJobSearchCount(jobSearchCount)
      .setJobDetailCount(jobDetailCount)
      .setAdDetailCount(adDetailCount)
      .setApplyJobCount(applyJobCount)
      .setFromRhash(fromYahoo)
      .setFromRhash(fromRhash)
      .setSessionEndTime(sessionEndTime)
      .setSsid(key)
      .build()
    out.collect(sessionEvent)
  }
}

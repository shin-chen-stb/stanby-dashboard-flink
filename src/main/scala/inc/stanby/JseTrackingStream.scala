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

import inc.stanby.StanbyAnalyticsStream.createStanbyEventSourceFromStaticConfig
import inc.stanby.operators.AmazonElasticsearchSink
import inc.stanby.schema._
import inc.stanby.serializers.{JseTrackerDeserializationSchema, StanbyEventDeserializationSchema}
import inc.stanby.windows.CalcSessionTimeWindowFunction
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties


object JseTrackingStream extends BasicStream {
  val logger: Logger = LoggerFactory.getLogger("Stanby_Dashboard_Flink");

  private def createJseTrackerSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKinesisConsumer[JseTracker]("stb-jse-tracker", new JseTrackerDeserializationSchema, inputProperties))
  }


  @throws[Exception]
  override def startStream(env: StreamExecutionEnvironment): Unit = {
    // set up the streaming execution environment
    val jseTrackerStream = createJseTrackerSourceFromStaticConfig(env)
    //   ------------------------------ JseTracking ------------------------------
    jseTrackerStream.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobSearchRequest")
    }).map { case x => jseTracker2JseTrackingJobSearchRequest(x) }
      .addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "jse-job-search", "_doc"))
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
      .setCityCode(event.getCityCode)
      .setStation(event.getStation)
      .setGeoLocation(event.getGeoLocation)
      .build
  }
}

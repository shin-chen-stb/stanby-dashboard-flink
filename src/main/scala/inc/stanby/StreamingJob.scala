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
import inc.stanby.schema.{JseTracker, StanbyEvent}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.http.HttpRequestInterceptor
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.functions.FilterFunction

import java.util.Properties
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import inc.stanby.schema.StanbyEvent
import inc.stanby.utils.{JseTrackerSchema, StanbyEventSchema}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.table.plan.nodes.datastream.TimeCharacteristic
import org.apache.flink.util.Collector

import java.io.IOException
import java.{lang, util}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
object StreamingJob {
  val serviceName = "es";
  val region = "ap-northeast-1";
  val domainEndpoint = "https://search-chen-stanby-analytics-dev-dqo5rvvb3udyugdkkrrzjr5gda.ap-northeast-1.es.amazonaws.com";
  val logger = LoggerFactory.getLogger("Stanby_Dashboard_Flink");

  private def createStanbyEventSourceFromStaticConfig(env: StreamExecutionEnvironment, inputStreamName: String) = {
    val inputProperties = new Properties
    inputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON")
    env.addSource(new FlinkKinesisConsumer[StanbyEvent](inputStreamName, new StanbyEventSchema, inputProperties))
  }

  private def createJseTrackerSourceFromStaticConfig(env: StreamExecutionEnvironment, inputStreamName: String) = {
    val inputProperties = new Properties
    inputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON")
    env.addSource(new FlinkKinesisConsumer[JseTracker](inputStreamName, new JseTrackerSchema, inputProperties))
  }


  @throws[Exception]
  def main(args: Array[String]): Unit = { // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = createStanbyEventSourceFromStaticConfig(env, "dmt-dataplatform-analytics-stream")
    val inputTemp = input.keyBy(new KeySelector[StanbyEvent, String] {
      override def getKey(event: StanbyEvent): String = event.getSsid.toString
    }).window(EventTimeSessionWindows.withGap(Time.minutes(5)))
      .process(new CalcSessionTimeWindowFunction())
    inputTemp.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_session_time", "_doc"))
    inputTemp.print()
    input.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event", "_doc"))
    val input2 = createJseTrackerSourceFromStaticConfig(env, "dmt-jse-tracker")
//    input2.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-tracker", "_doc"))
    input2.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobSearchRequest")
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-search", "_doc"))
    input2.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobDetailsImpression")
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-detail-impression", "_doc"))
    input2.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobImpression")
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-impression", "_doc"))
    input2.filter(new FilterFunction[JseTracker]() {
      @throws[Exception]
      override def filter(value: JseTracker): Boolean = value.getEventType.equals("jobClick")
    }).addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "dmt-jse-job-click", "_doc"))
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}

class CalcSessionTimeWindowFunction extends ProcessWindowFunction[StanbyEvent, (String, Long), String, TimeWindow] {
  val logger = LoggerFactory.getLogger("CalcSessionTimeWindowFunction");
  override def process(key: String, context: ProcessWindowFunction[StanbyEvent, (String, Long), String, TimeWindow]#Context, input: lang.Iterable[StanbyEvent], out: Collector[(String, Long)]) {
      var maxEpoch = 0L
      var minEpoch = Long.MaxValue
      val inputList = input.asScala
      for (in <- inputList) {
       minEpoch = math.min(minEpoch, in.getEpoch)
       maxEpoch = math.max(maxEpoch, in.getEpoch)
      }
      val res = (maxEpoch - minEpoch) / 1000
      logger.info("CalcSessionWindowResult: " + res.toString)
      out.collect(Tuple2(key, res))
  }
}

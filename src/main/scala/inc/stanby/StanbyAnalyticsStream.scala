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

import inc.stanby.JseTrackingStream.{domainEndpoint, region}
import inc.stanby.operators.AmazonElasticsearchSink
import inc.stanby.schema._
import inc.stanby.serializers.StanbyEventDeserializationSchema
import inc.stanby.windows.{CalcJseRequestKpiAggWindowFunction, CalcStanbyAnalyticsRequestKpiAggWindowFunction, CalcStanbyAnalyticsRequestKpiWindowFunction}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties


object StanbyAnalyticsStream extends BasicStream {
  val logger: Logger = LoggerFactory.getLogger("StanbyAnalyticsStreamLogger");

  private def createStanbyEventSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKinesisConsumer[StanbyEvent]("stb-dataplatform-analytics-stream", new StanbyEventDeserializationSchema, inputProperties))
  }

  @throws[Exception]
  override def startStream(env: StreamExecutionEnvironment): Unit = {
    // set up the streaming execution environment
    val StanbyEventStream = createStanbyEventSourceFromStaticConfig(env)

    //   ------------------------------ StanbyAnalyticsSession ------------------------------
//    val SessionWindowStream = StanbyEventStream.keyBy(new KeySelector[StanbyEvent, String] {
//      override def getKey(event: StanbyEvent): String = {
//        logger.info("GETKEY Event: " + event.getSsid.toString)
//        event.getSsid.toString
//      }
//    }).window(ProcessingTimeSessionWindows.withGap(Time.minutes(30)))
//      .process(new CalcSessionTimeWindowFunction())
//    SessionWindowStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_session", "_doc"))

    //   ------------------------------ StanbyAnalyticsSearchKpi ------------------------------
    val SearchKpiWindowStream = StanbyEventStream.filter(new FilterFunction[StanbyEvent]() {
      @throws[Exception]
      override def filter(value: StanbyEvent): Boolean = value.getSearchRequestId != null
    }).keyBy(new KeySelector[StanbyEvent, String] {
          override def getKey(event: StanbyEvent): String = {
        logger.info("GETKEY SearchRequestId: " + event.getSearchRequestId.toString)
        event.getSearchRequestId.toString
      }
    }).window(ProcessingTimeSessionWindows.withGap(Time.minutes(5)))
      .process(new CalcStanbyAnalyticsRequestKpiWindowFunction())
    val StanbyEventTumblingWindowStream = SearchKpiWindowStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .process(new CalcStanbyAnalyticsRequestKpiAggWindowFunction())
    SearchKpiWindowStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_search_kpi", "_doc"))
    StanbyEventTumblingWindowStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event_search_kpi_agg", "_doc"))
    //   ------------------------------ StanbyAnalytics ------------------------------
    StanbyEventStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event", "_doc"))
  }
}

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
import inc.stanby.serializers.AdTrackingDeserializationSchema
import inc.stanby.windows.CalcAdSearchKpiWindowFunction
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.slf4j.{Logger, LoggerFactory}


object AdTrackingStream extends BasicStream {
  val logger: Logger = LoggerFactory.getLogger("AdTrackingStreamLogger");

  private def createAdTrackerSourceFromStaticConfig(env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKinesisConsumer[AdTracking]("stb-ad-tracking", new AdTrackingDeserializationSchema(), inputProperties))
  }

  @throws[Exception]
  override def startStream(env: StreamExecutionEnvironment): Unit = {
    // set up the streaming execution environment
    val AdTrackingStream = createAdTrackerSourceFromStaticConfig(env)
    val AdSearchKpiWindowStream = AdTrackingStream.filter(new FilterFunction[AdTracking]() {
      @throws[Exception]
      override def filter(value: AdTracking): Boolean = value.getRequestId != null
    }).keyBy(new KeySelector[AdTracking, String] {
      override def getKey(event: AdTracking): String = {
        logger.info("GETKEY AdRequestId: " + event.getRequestId.toString)
        event.getRequestId.toString
      }
    }).window(ProcessingTimeSessionWindows.withGap(Time.seconds(300)))
      .process(new CalcAdSearchKpiWindowFunction())
    AdSearchKpiWindowStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "ad_search_kpi", "_doc"))

    //   ------------------------------ Ad-Tracking ------------------------------
    // Disable due to too much volume
//    AdTrackingStream.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "ad_tracking", "_doc"))
  }
}

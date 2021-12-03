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
import inc.stanby.schema.StanbyEvent
import inc.stanby.utils.StanbyEventSchema
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.http.HttpRequestInterceptor
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.Properties;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import inc.stanby.schema.StanbyEvent
import inc.stanby.utils.StanbyEventSchema
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import java.io.IOException
import java.util
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

  private def createSourceFromStaticConfig(env: StreamExecutionEnvironment, inputStreamName: String) = {
    val inputProperties = new Properties
    inputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON")
    env.addSource(new FlinkKinesisConsumer[String](inputStreamName, new SimpleStringSchema, inputProperties))
  }

  @throws[IOException]
  private def createSourceFromApplicationProperties(env: StreamExecutionEnvironment) = {
    val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
    env.addSource(new FlinkKinesisConsumer[StanbyEvent]("dmt-dataplatform-analytics-stream", new StanbyEventSchema, applicationProperties.get("ConsumerConfigProperties")))
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = { // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = createStanbyEventSourceFromStaticConfig(env, "dmt-dataplatform-analytics-stream")
    input.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, region, "stanby_event", "_doc"))

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}

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
import inc.stanby.serializers.JseTrackerDeserializationSchema
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties


abstract class BasicStream {
  val serviceName = "es";
  val region = "ap-northeast-1";
  val domainEndpoint = "https://vpc-stb-stream-dashboard-dz2o62ysvs7gqqkope7jnimagm.ap-northeast-1.es.amazonaws.com";
  val inputProperties = new Properties
  inputProperties.setProperty(AWSConfigConstants.AWS_REGION, region)
  inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

  def startStream(env: StreamExecutionEnvironment)
}

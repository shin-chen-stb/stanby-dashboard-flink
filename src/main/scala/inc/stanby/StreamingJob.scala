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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.{Logger, LoggerFactory}
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import java.util


object StreamingJob {
  val logger: Logger = LoggerFactory.getLogger("Stanby_Dashboard_Flink");

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
    val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
    val consumerProperties = applicationProperties.get("ConsumerConfigProperties")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    consumerProperties.get("name") match {
      case "stanby-analytics" => StanbyAnalyticsStream.startStream(env)
      case "jse-tracking" => JseTrackingStream.startStream(env)
      case "ad-tracking" => AdTrackingStream.startStream(env)
    }
    env.execute("Stanby KPI Streaming Application")
  }
}

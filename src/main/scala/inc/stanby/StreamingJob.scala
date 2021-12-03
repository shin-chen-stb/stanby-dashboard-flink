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

import inc.stanby.operators.AmazonElasticsearchSink;
import inc.stanby.schema.StanbyEvent;
import inc.stanby.utils.StanbyEventSchema;
import org.apache.flink.streaming.api.scala._
import inc.stanby.schema.StanbyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.http.HttpRequestInterceptor;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

object StreamingJob {
  val serviceName = "es";
  val region = "ap-northeast-1";
  val inputStreamName = "dmt-dataplatform-analytics-stream";
  val domainEndpoint = "https://search-chen-stanby-analytics-dev-dqo5rvvb3udyugdkkrrzjr5gda.ap-northeast-1.es.amazonaws.com";
  val logger = LoggerFactory.getLogger("Stanby_Dashboard_Flink");

  def createStanbyEventSourceFromStaticConfig(env: StreamExecutionEnvironment): DataStream[StanbyEvent] = {  
        val inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, this.region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");
        return env.addSource(new FlinkKinesisConsumer<>("dmt-dataplatform-analytics-stream", new StanbyEventSchema(), inputProperties));
  }

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = createStanbyEventSourceFromStaticConfig(env);
		input.addSink(AmazonElasticsearchSink.buildElasticsearchSink(domainEndpoint, this.region, "stanby_event2", "_doc", classOf[StanbyEvent]));

    

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}

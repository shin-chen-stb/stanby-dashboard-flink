package inc.stanby.operators;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;
import com.gilt.gfc.guava.GuavaConversions._

object AmazonElasticsearchSink {
  val ES_SERVICE_NAME = "es";

  val FLUSH_MAX_ACTIONS = 10000;
  val FLUSH_INTERVAL_MILLIS = 1000;
  val FLUSH_MAX_SIZE_MB = 1;

  val LOG = LoggerFactory.getLogger(classOf[AmazonElasticsearchSink]);

  def buildElasticsearchSink(elasticsearchEndpoint: String, region: String, indexName: String, esType: String, clsType: T): ElasticsearchSink[T] = {
    val httpHosts = Arrays.asList(HttpHost.create(elasticsearchEndpoint));
    val requestInterceptor = new SerializableAWSSigningRequestInterceptor(region);

    val esSinkBuilder = new ElasticsearchSink.Builder<>(
        httpHosts,
        new ElasticsearchSinkFunction[clsType]() {
          def createIndexRequest(element: String): IndexRequest = {
            return Requests.indexRequest()
                .index(indexName)
                .`type`(esType)
                .source(element.toString(), XContentType.JSON);
          }

          def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) = {
            indexer.add(createIndexRequest(element));
          }
        }
    );

    esSinkBuilder.setBulkFlushMaxActions(FLUSH_MAX_ACTIONS);
    esSinkBuilder.setBulkFlushInterval(FLUSH_INTERVAL_MILLIS);
    esSinkBuilder.setBulkFlushMaxSizeMb(FLUSH_MAX_SIZE_MB);
    esSinkBuilder.setBulkFlushBackoff(true);

    esSinkBuilder.setRestClientFactory(
        restClientBuilder -> restClientBuilder.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(requestInterceptor))
    );

    esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

    return esSinkBuilder.build();
  }


  object SerializableAWSSigningRequestInterceptor extends HttpRequestInterceptor with Serializable {
    @transient var requestInterceptor: AWSSigningRequestInterceptor = _;
    var region: String = _;
    
    def SerializableAWSSigningRequestInterceptor(region: String) {
      this.region = region;
    }

    override def process(httpRequest: HttpRequest, httpContext: HttpContext) = {
      if (requestInterceptor == null) {
        val clock: Supplier[LocalDateTime] = () => LocalDateTime.now(ZoneOffset.UTC)
        val credentialsProvider = new DefaultAWSCredentialsProviderChain();
        val awsSigner = new AWSSigner(credentialsProvider, region, ES_SERVICE_NAME, clock);

        requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);
      }

      requestInterceptor.process(httpRequest, httpContext);
    }
  }
}

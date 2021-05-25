package br.com.multivisao.producerKinesis.configs;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KinesisConfiguration {

    private String REGION = "us-west-1";
    private String STREAM_NAME = "xap";
    private String serviceEndpoint = "http://127.0.0.1:4566";

    @Bean
    public KinesisProducer configure(){
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setAggregationEnabled(true)
                .setAggregationMaxCount(12345678)
                .setAggregationMaxSize(51200)
                .setCollectionMaxCount(500)
                .setCollectionMaxSize(5242880)
                .setConnectTimeout(6000)
                .setKinesisEndpoint("localhost")
                .setFailIfThrottled(false)
                .setLogLevel("info")
                .setMaxConnections(24)
                .setMetricsGranularity("shard")
                .setMetricsLevel("detailed")
                .setMetricsNamespace("KinesisProducerLibrary")
                .setMetricsUploadDelay(60000)
                .setMinConnections(1)
                .setKinesisPort(443)
                .setRateLimit(150)
                .setRecordMaxBufferedTime(100)
                .setRecordTtl(30000)
                .setRegion("us-west-1")
                .setRequestTimeout(6000)
                .setVerifyCertificate(true)
                .setThreadingModel("PER_REQUEST");
        return new KinesisProducer(config);
    }

    @Bean
    public AmazonKinesis configureClient(){
        AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, REGION)).build();
        return kinesisClient;
    }
}

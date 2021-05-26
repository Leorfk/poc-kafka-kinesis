package br.com.multivisao.producerKinesis.configs;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KinesisConfiguration {

    @Value("${aws.access-key}")
    private String accessKey;
    @Value("${aws.secret-key}")
    private String secretKey;
    @Value("${aws.region}")
    private String region;


    public AWSCredentials getAwsCredentials() {
        return new BasicAWSCredentials(accessKey, secretKey);
    }

    @Bean
    public KinesisProducer createProducer(){
//        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
//                .setCredentialsProvider(new AWSStaticCredentialsProvider(getAwsCredentials()))
//                .setRegion(region);
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRecordMaxBufferedTime(15000)
                .setMaxConnections(1)
                .setRequestTimeout(60000)
                .setRegion("us-west-1")
                .setKinesisEndpoint("localhost")
                .setKinesisPort(4566)
                .setVerifyCertificate(false)
                .setCredentialsProvider(new AWSStaticCredentialsProvider(getAwsCredentials()));
        return new KinesisProducer(config);
    }
}

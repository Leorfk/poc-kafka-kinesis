package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.configs.KinesisConfiguration;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
public class KinesisProducerService {

    @Autowired
    private KinesisConfiguration kinesisConfiguration;

    private String REGION = "us-west-1";
    private String STREAM_NAME = "xap";
    private String serviceEndpoint = "http://127.0.0.1:4566";

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    public FutureCallback<UserRecordResult> acao(){
        return new FutureCallback<UserRecordResult>() {
            @Override public void onFailure(Throwable t) {
                System.out.println("Deu ruim");
            };
            @Override public void onSuccess(UserRecordResult result) {
                System.out.println("Deu bom");
            };
        };
    }

    public void produce() throws UnsupportedEncodingException {
        System.out.println("Chegamo aqui no produce");
        final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
        FutureCallback<UserRecordResult> myCallback = acao();
        var kinesis = kinesisConfiguration.configure();
        for (int i = 0; i < 5; ++i) {
            ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
            ListenableFuture<UserRecordResult> f = kinesis.addUserRecord("xap", String.valueOf(i), data);
            // If the Future is complete by the time we call addCallback, the callback will be invoked immediately.
            Futures.addCallback(f, myCallback, callbackThreadPool);
        }
    }

    public void mamus() {
        Region region = Region.getRegion(Regions.fromName(REGION));
        var kinesisClient = kinesisConfiguration.configureClient();
//        AmazonKinesis kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain()).withRegion(region);
//        AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard()
//                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, REGION)).build();
        ListStreamsResult listStreamsResult;
        try {
            listStreamsResult = kinesisClient.listStreams();
        } catch (AmazonServiceException ase) {
            System.out.println("Exception while list streams");
            System.out.println("-----------------------------");
            ase.printStackTrace(System.err);
            System.out.println(ase.getMessage());
            System.out.println("-----------------------------");
            System.out.println("Failed to list streams due to " + STREAM_NAME);
            System.out.println("Unable to list your streams in " + REGION);
            String kinesisEndpoint = region.getServiceEndpoint("kinesis");
            System.out.println("Please make sure that credentials are available to the demo, and you have connectivity to reach " + kinesisEndpoint);

            throw new RuntimeException("Can't connect to Kinesis to retrieve stream names", ase);
        }

        Set<String> streamNames = new HashSet<>();
        streamNames.addAll(listStreamsResult.getStreamNames());

        if (!streamNames.contains(STREAM_NAME)) {
            System.out.println("No stream names "+STREAM_NAME+" exists in region "+ region.getName());
            System.out.println("Was the stream created in another region?");

            throw new RuntimeException("Stream '" + STREAM_NAME + "' doesn't exist");
        }

        DescribeStreamResult describeStreamResult;
        try {
            describeStreamResult = kinesisClient.describeStream(STREAM_NAME);
        } catch (ResourceNotFoundException rnfe) {
            System.out.println("Exception while trying to describe stream  "+STREAM_NAME+" in region "+region.getName());
            System.out.println("-----------------------------");
            rnfe.printStackTrace(System.err);
            System.out.println("-----------------------------");
            System.out.println("Stream "+STREAM_NAME+" was removed in between calls");

            throw new RuntimeException(rnfe);
        } catch (AmazonServiceException ase) {
            System.out.println("Exception while trying to describe stream "+STREAM_NAME+" in region "+region.getName());
            System.out.println("-----------------------------");
            ase.printStackTrace(System.err);
            System.out.println("-----------------------------");
            System.out.println(
                    "It's possible that the provided credentials don't have access rights to call describe stream.");

            throw new RuntimeException(ase);
        }
        StreamStatus status = StreamStatus.fromValue(describeStreamResult.getStreamDescription().getStreamStatus());
        if (status != StreamStatus.ACTIVE) {
            System.out.println("The stream "+STREAM_NAME+" exists, but isn't active.  "
                    + "Please wait for the stream to become active and try again.");

            throw new RuntimeException("Stream not active.");
        }
        System.out.println("Sem erros, pode começar !!! :)");

    }
}

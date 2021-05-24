package br.com.multivisao.producerKinesis.services;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class KinesisProducerService {

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
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setAggregationEnabled(true)
                .setAggregationMaxCount(12345678)
                .setAggregationMaxSize(51200)
                .setCollectionMaxCount(500)
                .setCollectionMaxSize(5242880)
                .setConnectTimeout(6000)
//                .setKinesisEndpoint("kinesis:sa-east-1:123456789012:stream/xap")
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
                .setRegion("sa-west-1")
                .setRequestTimeout(6000)
                .setVerifyCertificate(true)
                .setThreadingModel("PER_REQUEST");
        KinesisProducer kinesis = new KinesisProducer(config);
        final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
        FutureCallback<UserRecordResult> myCallback = acao();

        for (int i = 0; i < 100; ++i) {
            ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
            ListenableFuture<UserRecordResult> f = kinesis.addUserRecord("xap", i + "toma", data);
            // If the Future is complete by the time we call addCallback, the callback will be invoked immediately.
            Futures.addCallback(f, myCallback, callbackThreadPool);
        }

    }
}

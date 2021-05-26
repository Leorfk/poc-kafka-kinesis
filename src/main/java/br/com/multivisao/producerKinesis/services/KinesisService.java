package br.com.multivisao.producerKinesis.services;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
public class KinesisService {

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    public void putRecords(String data, String partitionKey, String streamName, KinesisProducer kinesis) throws UnsupportedEncodingException {
        FutureCallback<UserRecordResult> myCallback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                System.err.println("Deu ruim. " + t);
            };

            @Override
            public void onSuccess(UserRecordResult result) {
                System.err.println("Deu bom, resultado: " +
                        "\nShard ID: " + result.getShardId() +
                        "\nSequence number: " +  result.getSequenceNumber());
                kinesis.flushSync();
                kinesis.destroy();
            };
        };
        final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();
        ByteBuffer records = ByteBuffer.wrap(data.getBytes("UTF-8"));
        System.out.println("Dados compactados" + records);
        ListenableFuture<UserRecordResult> future = kinesis
                .addUserRecord(streamName, partitionKey, records);
        // If the Future is complete by the time we call addCallback, the callback will
        // be invoked immediately.
        Futures.addCallback(future, myCallback, callbackThreadPool);
    }
}

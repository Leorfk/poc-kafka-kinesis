package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.configs.KinesisConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KinesisService {

    @Autowired
    private KinesisConfiguration kinesisConfiguration;
    @Value("aws.kinesis.stream-name")
    private String streamName;

    private final ExecutorService callbackThreadPool = Executors.newCachedThreadPool();

    public void putRecords(String data, String partitionKey) {
        var kinesis = kinesisConfiguration.createProducer();
        ByteBuffer records = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
        System.out.println("Dados compactados" + records);
        ListenableFuture<UserRecordResult> future = kinesis
                .addUserRecord(streamName, partitionKey, records);
        Futures.addCallback(future, myCallback(), callbackThreadPool);
    }

    private FutureCallback<UserRecordResult> myCallback() {
        return new FutureCallback<UserRecordResult>() {

            public void onFailure(Throwable t) {
                System.err.println("Deu ruim. " + t);
            };

            public void onSuccess(UserRecordResult result) {
                if (result != null) {
                    System.err.println("Deu bom, resultado: " +
                            "\nShard ID: " + result.getShardId() +
                            "\nSequence number: " +  result.getSequenceNumber());
                } else {
                    System.err.println("Deu certo mas foi errado");
                }
            };
        };
    }
}

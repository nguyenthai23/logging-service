package vn.example.logging_service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.example.logging_service.constants.IKafkaConstants;
import vn.example.logging_service.consumer.ConsumerCreator;

import java.sql.Timestamp;
import java.time.Duration;

public class LoggingApp {
    private final Logger logger = LoggerFactory.getLogger(LoggingApp.class);

    public static void main(String[] args) {
        LoggingApp loggingApp = new LoggingApp();
        loggingApp.runConsumer();
    }

    private void runConsumer() {
        Consumer<Long, Object> consumer = ConsumerCreator.createConsumer();
        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, Object> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
//					break;
                    try {
                        Thread.sleep(1000);
                        noMessageToFetch = 0;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else
                    continue;
            }

            consumerRecords.forEach(record -> showLog(record));
            consumer.commitAsync();
        }
    }

    private void showLog(ConsumerRecord record) {
        int partition = record.partition();
        String timestamp = new Timestamp(record.timestamp()).toLocalDateTime().toString();
        if (partition == IKafkaConstants.PARTTITON_BEFOR_CREATE) {
            logger.info(timestamp + "- BEFOR_CREATE: " + record.value());
            return;
        }

        if (partition == IKafkaConstants.PARTTITON_AFTER_CREATE) {
            logger.info(timestamp + "- AFTER_CREATE: " + record.value());
            return;
        }

        if (partition == IKafkaConstants.PARTTITON_BEFOR_UPDATE) {
            logger.info(timestamp + "- BEFOR_UPDATE: " + record.value());
            return;
        }

        if (partition == IKafkaConstants.PARTTITON_AFTER_UPDATE) {
            logger.info(timestamp + "- AFTER_UPDATE: " + record.value());
            return;
        }

        if (partition == IKafkaConstants.PARTTITON_BEFOR_DELETE) {
            logger.info(timestamp + "- BEFOR_DELETE: " + record.value());
            return;
        }

        if (partition == IKafkaConstants.PARTTITON_AFTER_DELETE) {
            logger.info(timestamp + "- AFTER_DELETE: " + record.value());
            return;
        }
    }
}

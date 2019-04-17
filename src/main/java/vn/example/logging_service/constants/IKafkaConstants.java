package vn.example.logging_service.constants;

public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "localhost:9092";

    public static Integer MESSAGE_COUNT = 1000;

    public static String CLIENT_ID = "client1";

    public static String TOPIC_NAME = "demo";

    public static Integer PARTTITON_BEFOR_CREATE = 0;
    public static Integer PARTTITON_AFTER_CREATE = 1;

    public static Integer PARTTITON_BEFOR_UPDATE = 3;
    public static Integer PARTTITON_AFTER_UPDATE = 4;

    public static Integer PARTTITON_BEFOR_DELETE = 5;
    public static Integer PARTTITON_AFTER_DELETE = 6;

    public static String GROUP_ID_CONFIG = "consumerGroup10";

    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;

    public static String OFFSET_RESET_LATEST = "latest";

    public static String OFFSET_RESET_EARLIER = "earliest";

    public static Integer MAX_POLL_RECORDS = 1;
}

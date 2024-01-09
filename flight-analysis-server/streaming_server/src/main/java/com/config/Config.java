package com.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Config {
    public static String APP_NAME = "streaming_data_server";

    public static int LISTEN_TIME = 5;

    public static String TCP_SERVER_IP = "127.0.0.1";
    public static int TCP_SERVER_PORT = 9168;

    public static String MYSQL_IP = "sh-cynosdbmysql-grp-5tpsbo9c.sql.tencentcdb.com";
    public static String MYSQL_USERNAME = "flight_analysis_user";
    public static String MYSQL_PASSWORD = "123456azM";
    public static int MYSQL_PORT = 26211;
    public static String MYSQL_DBNAME = "flight_analysis";

    public static String SPARK_IP = "spark://192.168.145.131:7077";

    public static String KAFKA_TOPIC = "heima";
    public static String KAFKA_IP_1 = "192.168.145.131:9092";
    public static String KAFKA_IP_2 = "192.168.145.132:9092";
    public static String KAFKA_IP_3 = "192.168.145.133:9092";

    public static String KAFKA_IP_ALL(){
        return KAFKA_IP_1 + "," + KAFKA_IP_2 + "," + KAFKA_IP_3;
    }

    public static String ZOOKEEPER_QUORUM = "master,node1,node2";
    public static String ZOOKEEPER_CLIENT_PORT = "2181";
    public static String HBASE_TABLE_NAME = "test";

    public static SparkConf sparkConf() throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String currentIp = inetAddress.getHostAddress();
        return new SparkConf()
                .setAppName(Config.APP_NAME)
                .setMaster(Config.SPARK_IP)
                .set("spark.driver.host", currentIp)
                .set("spark.driver.port", "9999")
                .setJars(new String[]{"C:\\Users\\86178\\Desktop\\flight-analysis\\flight-analysis-server\\streaming_server\\target\\streaming_server-0.0.1-SNAPSHOT.jar",
                });
    }


    public static org.apache.hadoop.conf.Configuration hbaseConf(){
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        hbaseConf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENT_PORT);
        hbaseConf.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE_NAME);
        return hbaseConf;
    }

    public static Map<String, Object>  streamingConnectKafkaConf(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_IP_ALL());
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "use_a_separate_group_id_for_each_stream");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return kafkaParams;
    }

    public static Properties kafkaClientConf(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_IP_ALL());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }
}

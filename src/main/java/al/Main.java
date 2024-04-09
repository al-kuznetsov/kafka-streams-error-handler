package al;

import java.util.Properties;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import al.streams.TopologyBuilder;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Started main");

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-error-handler");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        // settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        // StreamsConfig.EXACTLY_ONCE_V2);

        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        settings.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        settings.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        settings.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"nana\" password=\"nana-secret\";");
        // settings.put(SaslConfigs.SASL_JAAS_CONFIG,
        //         "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"ivan\" password=\"ivan-secret\";");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.buildStreams(streamsBuilder);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), settings);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
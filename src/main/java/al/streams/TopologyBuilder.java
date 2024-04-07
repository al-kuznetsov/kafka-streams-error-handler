package al.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import al.constant.TopicNames;

public class TopologyBuilder {
    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

    public void buildStreams(StreamsBuilder streamsBuilder) {
        KStream<String, String> input = streamsBuilder.stream(TopicNames.inputTopic);

        input
                .peek((k, v) -> log.info("Reading input record k = {}, v = {}", k, v))
                .split()
                .branch((k, v) -> v.contains("type1"),
                        Branched.withConsumer(ks -> ks
                                .peek((k, v) -> log.info("Branched record to output topic k = {}, v = {}", k, v))
                                .to(TopicNames.outputTopic)))
                .branch((k, v) -> v.contains("typeE"),
                        Branched.withConsumer(ks -> ks
                                .peek((k, v) -> log.info("Branched record to WRONG topic k = {}, v = {}", k, v))
                                .to((k, v, c) -> TopicNames.wrongTopic)))
                .noDefaultBranch();

    }

}

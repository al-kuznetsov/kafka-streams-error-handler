package al.processor;

import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandlerProcessor implements Processor<String, String, String, String> {
    private static final Logger log = LoggerFactory.getLogger(ErrorHandlerProcessor.class);

    private final String destinationTopic;
    private ProcessorContext<String, String> context;

    public ErrorHandlerProcessor(String destinationTopic) {
        this.destinationTopic = destinationTopic;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        log.info("Start processing in Error Handler");
        try {
            context.forward(record);
        } catch (TopicAuthorizationException e) {
            log.warn("TopicAuthorizationException hit!");
        } catch (Exception e) {
            log.warn("Exception hit", e);
        }

    }

}

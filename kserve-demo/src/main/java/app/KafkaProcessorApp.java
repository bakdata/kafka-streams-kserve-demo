package app;


import client.KFServingClientFactory;
import client.KFServingClientFactoryV1;
import client.KFServingClientFactoryV2;
import com.bakdata.kafka.ErrorCapturingValueMapper;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.ProcessedValue;
import com.bakdata.kafka.ProcessingError;
import com.google.common.reflect.TypeToken;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine;
import predictv2.InferenceRequest;
import predictv2.InferenceResponse;
import serde.JsonPOJODeserializer;
import serde.JsonPOJOSerializer;
import types.ProtocolVersion;

@Getter
@NoArgsConstructor
public abstract class KafkaProcessorApp<I, P, O> extends KafkaStreamsApplication {

    private final JsonPOJOSerializer<I> inputSerializer = new JsonPOJOSerializer<I>();
    private final JsonPOJODeserializer<I> inputDeserializer = getInputDynamicDeserializer();
    private final JsonPOJOSerializer<O> outputSerializer = new JsonPOJOSerializer<O>();
    private final JsonPOJODeserializer<O> outputDeserializer = getOutputDynamicDeserializer();
    @CommandLine.Option(names = "--model-name", required = true)
    private String modelName;
    @CommandLine.Option(names = "--inference-service", required = true)
    private String inferenceService;
    @CommandLine.Option(names = "--base-endpoint", required = true)
    private String baseEndpoint;
    @CommandLine.Option(names = "--protocol-version", required = true)
    private ProtocolVersion protocolVersion;
    private KfServingRequester<InferenceRequest<I, O>, P> requester;

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        Serde<I> inputSerde = getInputSerde();
        final KStream<byte[], I> input =
                builder.<byte[], I>stream(this.getInputTopics(),
                        Consumed.with(Serdes.ByteArray(), inputSerde));

        KStream<byte[], ProcessedValue<I, O>> processedValues =
                input.mapValues(ErrorCapturingValueMapper.captureErrors(this::process));

        Serde<O> outputSerde = getOutputSerde();
        processedValues.flatMapValues(ProcessedValue::getValues)
                .to(this.getOutputTopic(), Produced.with(Serdes.ByteArray(), outputSerde));

        processedValues.flatMapValues(ProcessedValue::getErrors)
                .mapValues(x -> x.createDeadLetter("Error in infer"))
                .to(this.getErrorTopic());
    }

    @Override
    public String getUniqueAppId() {
        return "streams-bootstrap-app";
    }

    protected KfServingRequester<InferenceRequest<I, O>, P> createRequester() {
        TypeToken<P> type = new TypeToken<P>(getClass()) {};
        return new KfServingRequester(getProtocolFactory(), this.inferenceService, this.baseEndpoint, this.modelName,
                type.getRawType());
    }

    abstract protected O process(I input);

    // Optionally you can override the default streams bootstrap Kafka properties
    @Override
    protected Properties createKafkaProperties() {
        this.requester = createRequester();
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, ByteArraySerde.class);
        return kafkaProperties;
    }

    private JsonPOJODeserializer<I> getInputDynamicDeserializer() {
        TypeToken<I> type = new TypeToken<I>(getClass()) {};
        JsonPOJODeserializer<I> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(Map.of("JsonPOJOClass", (Class<I>) type.getRawType()), false);
        return deserializer;
    }

    private JsonPOJODeserializer<O> getOutputDynamicDeserializer() {
        TypeToken<O> type = new TypeToken<O>(getClass()) {};
        JsonPOJODeserializer<O> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(Map.of("JsonPOJOClass", (Class<O>) type.getRawType()), false);
        return deserializer;
    }

    private KFServingClientFactory getProtocolFactory() {
        switch (this.protocolVersion) {
            case V1:
                return new KFServingClientFactoryV1();
            case V2:
                return new KFServingClientFactoryV2();
            default:
                throw new RuntimeException("Wrong protocol type given");
        }
    }

    @NotNull
    private Serde<O> getOutputSerde() {
        return Serdes.serdeFrom(outputSerializer, outputDeserializer);
    }

    @NotNull
    private Serde<I> getInputSerde() {
        return Serdes.serdeFrom(inputSerializer, inputDeserializer);
    }


}
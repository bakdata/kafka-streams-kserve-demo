/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package app;

import com.bakdata.kafka.ErrorCapturingValueMapper;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.ProcessedValue;
import com.bakdata.kserve.client.KServeClientFactory;
import com.bakdata.kserve.client.KServeClientFactoryV1;
import com.bakdata.kserve.client.KServeClientFactoryV2;
import com.bakdata.kserve.predictv2.InferenceRequest;
import com.google.common.reflect.TypeToken;
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
import serde.JsonPOJODeserializer;
import serde.JsonPOJOSerializer;
import types.ProtocolVersion;

import java.util.Map;
import java.util.Properties;

@Getter
@NoArgsConstructor
public abstract class KafkaProcessorApp<I, P, O> extends KafkaStreamsApplication {

    private final JsonPOJOSerializer<I> inputSerializer = new JsonPOJOSerializer<I>();
    private final JsonPOJODeserializer<I> inputDeserializer = this.getInputDynamicDeserializer();
    private final JsonPOJOSerializer<O> outputSerializer = new JsonPOJOSerializer<O>();
    private final JsonPOJODeserializer<O> outputDeserializer = this.getOutputDynamicDeserializer();
    @CommandLine.Option(names = "--model-name", required = true, description = "The model name as defined in model-settings.json for the custom predictor")
    private String modelName;
    @CommandLine.Option(names = "--inference-service-name", required = true, description = "The name of the inference service")
    private String inferenceServiceName;
    @CommandLine.Option(names = "--base-endpoint", required = true, description = "The base URL of the KServe inference service namespace. http://{$APP_INFERENCE_SERVICE_NAME}{$APP_BASE_ENDPOINT} should match the URL of the translator inference service")
    private String baseEndpoint;
    @CommandLine.Option(names = "--protocol-version", required = true, description = "The KServe inference protocol version")
    private ProtocolVersion protocolVersion;
    private KServeRequester<InferenceRequest<I>, P> requester = null;

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final Serde<I> inputSerde = this.getInputSerde();
        final KStream<byte[], I> input =
                builder.stream(this.getInputTopics(),
                        Consumed.with(Serdes.ByteArray(), inputSerde));

        final KStream<byte[], ProcessedValue<I, O>> processedValues =
                input.mapValues(ErrorCapturingValueMapper.captureErrors(this::process));

        final Serde<O> outputSerde = this.getOutputSerde();
        processedValues.flatMapValues(ProcessedValue::getValues)
                .to(this.getOutputTopic(), Produced.with(Serdes.ByteArray(), outputSerde));

        processedValues.flatMapValues(ProcessedValue::getErrors)
                .mapValues(x -> x.createDeadLetter("Error in infer"))
                .to(this.getErrorTopic());
    }

    protected KServeRequester<InferenceRequest<I>, P> createRequester() {
        final TypeToken<P> type = new TypeToken<P>(this.getClass()) {};
        return new KServeRequester(this.getProtocolFactory(), this.inferenceServiceName, this.baseEndpoint, this.modelName,
                type.getRawType());
    }

    protected abstract O process(I input);

    // Optionally you can override the default streams bootstrap Kafka properties
    @Override
    protected Properties createKafkaProperties() {
        this.requester = this.createRequester();
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, ByteArraySerde.class);
        return kafkaProperties;
    }

    private JsonPOJODeserializer<I> getInputDynamicDeserializer() {
        final TypeToken<I> type = new TypeToken<I>(this.getClass()) {};
        final JsonPOJODeserializer<I> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(Map.of("JsonPOJOClass", (Class<I>) type.getRawType()), false);
        return deserializer;
    }

    private JsonPOJODeserializer<O> getOutputDynamicDeserializer() {
        final TypeToken<O> type = new TypeToken<O>(this.getClass()) {};
        final JsonPOJODeserializer<O> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(Map.of("JsonPOJOClass", (Class<O>) type.getRawType()), false);
        return deserializer;
    }

    private KServeClientFactory getProtocolFactory() {
        switch (this.protocolVersion) {
            case V1:
                return new KServeClientFactoryV1();
            case V2:
                return new KServeClientFactoryV2();
            default:
                throw new RuntimeException("Wrong protocol type given");
        }
    }

    @NotNull
    private Serde<O> getOutputSerde() {
        return Serdes.serdeFrom(this.outputSerializer, this.outputDeserializer);
    }

    @NotNull
    private Serde<I> getInputSerde() {
        return Serdes.serdeFrom(this.inputSerializer, this.inputDeserializer);
    }

}
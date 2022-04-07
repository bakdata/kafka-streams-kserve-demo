package app;

import java.util.Collection;
import java.util.List;

import com.bakdata.kserve.predictv2.InferenceRequest;
import com.bakdata.kserve.predictv2.InferenceResponse;
import com.bakdata.kserve.predictv2.Parameters;
import com.bakdata.kserve.predictv2.RequestInput;
import com.bakdata.kserve.predictv2.ResponseOutput;
import types.TranslateResponse;
import types.Translation;
import types.TextTranslation;

public class App extends KafkaProcessorApp<TextTranslation, TranslateResponse, Translation>{

    @Override
    protected Translation process(TextTranslation input) {
        return this.getRequester().requestInferenceService(InferenceRequest.<TextTranslation, Translation>builder()
                        .inputs(List.of(
                                RequestInput.<TextTranslation>builder()
                                        .name("Translation")
                                        .datatype("BYTES")
                                        .shape(List.of(1))
                                        .datatype("BYTES")
                                        .parameters(Parameters.builder()
                                                .content_type("str")
                                                .build())
                                        .data(input)
                                        .build()
                        ))
                .build())
                .map(InferenceResponse::getOutputs)
                .stream()
                .flatMap(Collection::stream)
                .map(ResponseOutput::getData)
                .findFirst()
                .orElseThrow();
    }

    public static void main(String[] args) {
        startApplication(new App(), args);
    }

    @Override
    public String getUniqueAppId() {
        return "kafka-streams-app";
    }
}

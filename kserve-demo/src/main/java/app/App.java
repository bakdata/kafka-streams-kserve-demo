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

import com.bakdata.kserve.predictv2.InferenceRequest;
import com.bakdata.kserve.predictv2.InferenceResponse;
import com.bakdata.kserve.predictv2.Parameters;
import com.bakdata.kserve.predictv2.RequestInput;
import com.bakdata.kserve.predictv2.ResponseOutput;
import types.TextToTranslate;
import types.Translation;
import types.TranslatorResponse;

import java.util.Collection;
import java.util.List;

public class App extends KafkaProcessorApp<TextToTranslate, TranslatorResponse, Translation>{

    @Override
    protected Translation process(final TextToTranslate input) {
        return this.getRequester().requestInferenceService(InferenceRequest.<TextToTranslate>builder()
                        .inputs(List.of(
                                RequestInput.<TextToTranslate>builder()
                                        .name("Translation")
                                        .datatype("BYTES")
                                        .shape(List.of(1))
                                        .datatype("BYTES")
                                        .parameters(Parameters.builder()
                                                .contentType("str")
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

    public static void main(final String[] args) {
        startApplication(new App(), args);
    }

    @Override
    public String getUniqueAppId() {
        return "kafka-streams-translator";
    }
}

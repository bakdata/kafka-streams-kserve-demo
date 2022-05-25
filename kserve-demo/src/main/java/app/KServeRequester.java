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


import com.bakdata.kserve.client.KServeClient;
import com.bakdata.kserve.client.KServeClientFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;


public class KServeRequester<I, O> {
    private static final Duration REQUEST_READ_TIMEOUT =
            Optional.ofNullable(System.getenv("KSERVE_REQUEST_READ_TIMEOUT"))
                    .map(Integer::parseInt).map(Duration::ofMillis)
                    .orElse(Duration.ofMillis(10000));
    private final KServeClient<I> kServeClient;
    private final Class<? extends O> type;


    protected KServeRequester(
            final KServeClientFactory kServeClientFactory,
            final String inferenceServiceName, final String baseEndpoint, final String modelName,
            final Duration requestReadTimeout,
            final Class<? extends O> type) {

        this.type = type;
        this.kServeClient = (KServeClient<I>) kServeClientFactory.getKServeClient(
                String.format("%s%s", inferenceServiceName, baseEndpoint),
                modelName,
                REQUEST_READ_TIMEOUT.compareTo(requestReadTimeout) > 0 ?
                        REQUEST_READ_TIMEOUT : requestReadTimeout,
                false);
    }

    protected KServeRequester(
            final KServeClientFactory kServeClientFactory,
            final String inferenceServiceName, final String baseEndpoint, final String modelName,
            final Class<? extends O> type) {
        this(kServeClientFactory, inferenceServiceName, baseEndpoint, modelName,
                REQUEST_READ_TIMEOUT, type);
    }


    private static boolean getKServeRuntimeEnvironment() {
        return Optional.ofNullable(System.getenv("KSERVE_ENV")).map("local"::equals).orElse(false);
    }

    protected Optional<O> requestInferenceService(final I jsonObject) {
        try {
            return this.kServeClient.makeInferenceRequest(jsonObject, this.type, "");
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    "Error occurred when sending or receiving the inference request/response", e);
        }
    }
}

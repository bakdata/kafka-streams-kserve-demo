package app;


import com.bakdata.kserve.client.KFServingClient;
import com.bakdata.kserve.client.KFServingClientFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;


public class KfServingRequester<I, O> {
    private static final Duration REQUEST_READ_TIMEOUT =
            Optional.ofNullable(System.getenv("KFSERVING_REQUEST_READ_TIMEOUT"))
                    .map(Integer::parseInt).map(Duration::ofMillis)
                    .orElse(Duration.ofMillis(10000));
    private final KFServingClient<I> kfServingClient;
    private final Class<? extends O> type;


    protected KfServingRequester(
            final KFServingClientFactory kfServingClientFactory,
            final String inferenceServiceName, final String baseEndpoint, final String modelName,
            final Duration requestReadTimeout,
            final Class<? extends O> type) {

        this.type = type;
        this.kfServingClient = (KFServingClient<I>) kfServingClientFactory.getKFServingClient(
                String.format("%s%s", inferenceServiceName, baseEndpoint),
                modelName,
                REQUEST_READ_TIMEOUT.compareTo(requestReadTimeout) > 0 ?
                        REQUEST_READ_TIMEOUT : requestReadTimeout);
    }

    protected KfServingRequester(
            final KFServingClientFactory kfServingClientFactory,
            final String inferenceServiceName, final String baseEndpoint, final String modelName,
            final Class<? extends O> type) {
        this(kfServingClientFactory, inferenceServiceName, baseEndpoint, modelName,
                REQUEST_READ_TIMEOUT, type);
    }


    private static boolean getKFServingRuntimeEnvironment() {
        return Optional.ofNullable(System.getenv("KFSERVING_ENV")).map("local"::equals).orElse(false);
    }

    protected Optional<O> requestInferenceService(final I jsonObject) {
        return this.requestInferenceService(jsonObject, "");
    }

    protected Optional<O> requestInferenceService(final I jsonObject, final String modelNameSuffix) {
        try {
            return this.kfServingClient.makeInferenceRequest(jsonObject, this.type, modelNameSuffix);
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    "Error occurred when sending or receiving the inference request/response", e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("The operation was interrupted", e);
        }
    }

}

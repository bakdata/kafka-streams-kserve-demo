import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.endpoints.AdditionalParameters;
import io.github.redouane59.twitter.dto.tweet.TweetList;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import types.TextTranslation;

public class TextProducer {

    public static void main(String[] args) throws URISyntaxException, IOException {

        String brokers = System.getenv().getOrDefault("BROKERS", "");
        String topicName = System.getenv().getOrDefault("KAFKA_TOPIC", "to-translate");
        String twitterToken = System.getenv().getOrDefault("TWITTER_TOKEN", "");
        String maxResults = System.getenv().getOrDefault("MAX_RESULTS", "50");
        String searchString = System.getenv().getOrDefault("SEARCH", "");

        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "serde.JsonPOJOSerializer");

        org.apache.kafka.clients.producer.Producer<String, TextTranslation> producer =
                new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        List<String> splitted = Arrays.asList(searchString.split(","));
        for (String substring : splitted
        ) {
            TwitterClient twitterClient = new TwitterClient(TwitterCredentials.builder()
                    .bearerToken(twitterToken)
                    .build());

            TweetList result = twitterClient.searchTweets(substring,
                    AdditionalParameters.builder()
                            .recursiveCall(false)
                            .maxResults(Integer.parseInt(maxResults)).build());

            for (int i = 0; i < result.getData().size(); i++) {

                TextTranslation toTranslate = TextTranslation
                        .builder()
                        .textToTranslate(result.getData().get(i).getText())
                        .build();

                producer.send(new ProducerRecord<>(topicName,
                        Integer.toString(i), toTranslate));
            }
        }

        System.out.println("Messages sent successfully");
        producer.close();
    }
}

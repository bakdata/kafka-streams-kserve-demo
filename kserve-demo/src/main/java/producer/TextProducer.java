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

package producer;

import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.endpoints.AdditionalParameters;
import io.github.redouane59.twitter.dto.tweet.TweetList;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import org.apache.kafka.clients.producer.ProducerRecord;
import types.TextToTranslate;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TextProducer {

    public static void main(String[] args) throws URISyntaxException, IOException {

        String brokers = System.getenv().getOrDefault("APP_BROKERS", "");
        String topicName = System.getenv().getOrDefault("APP_OUTPUT_TOPIC", "to-translate");
        // A Twitter API Bearer Token
        String twitterToken = System.getenv().getOrDefault("APP_TWITTER_TOKEN", "");
        // Max number of results returned by the Twitter API per search term
        String maxResults = System.getenv().getOrDefault("APP_MAX_RESULTS", "50");
        // List of Twitter tweet search terms (comma-separated)
        String searchString = System.getenv().getOrDefault("APP_SEARCH", "");

        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "serde.JsonPOJOSerializer");

        org.apache.kafka.clients.producer.Producer<String, TextToTranslate> producer =
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

                TextToTranslate toTranslate = TextToTranslate
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

# kafka-streams-kserve-demo

A demo to accompany a blogpost about [Scalable Machine Learning with Kafka Streams and KServe](https://medium.com/bakdata/xyz). More information on how to run the demo can be found in the blogpost.

The three Docker images for the demo have been published on the [bakdata Docker Hub page](https://hub.docker.com/u/bakdata). They are automatically pulled if you follow the setup instructions in the blogpost.

In case you want to modify any of the three components, you can build your own version. There is a `mlserver-translator/Dockerfile` for the `kserve-demo-mlserver-translator` image. JIB build steps for the `kserve-demo-text-producer` and `kserve-demo-kafka-streams-app` images can be found in `kserve-demo/build.gradle`.

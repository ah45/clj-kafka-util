# clj-kafka-util

A collection of utility fns related to [clj-kafka]

[clj-kafka]: https://github.com/pingles/clj-kafka

## Installing

Add the following to your `project.clj`:

[![Clojars Project](http://clojars.org/org.clojars.ah45/clj-kafka-util/latest-version.svg)](http://clojars.org/org.clojars.ah45/clj-kafka-util)

## Usage

Currently provides two functions of note under the
`clj-kafka.consumer.util` namespace:

* `topic->reducable`

  Returns a reduceable wrapper around a topic stream that gracefully
  handles consumer timeouts and abstracts away the consumer/stream
  creation making it easy to treat Kafka topics as just another
  collection type:

  ```clojure
  ;; build map of messages from a topic, note the timeout
  (reduce
    (fn [rs m]
      (assoc rs (.key m) (.message m)))
    {}
    (topic->reduceable
      "my_topic"
      (assoc kafka-config "consumer.timeout.ms" "30")))

  ;; collect all message up to a known offset
  (let [max-offset 100]
    ;; via reduction
    (reduce
      (fn [rs m]
        (cond
          (> (.offset m) max-offset) (reduced rs)
          (= (.offset m) max-offset) (reduced (conj rs m))
          :else (conj rs m)))
      []
      (topic->reduceable "my_topic" kafka-config)))
    ;; or transduction (simpler!)
    (transduce
      (take-while #(<= (.offset %) max-offset))
      conj []
      (topic->reduceable "my_topic" kafka-config))

  ;; get the content of the next 100 messages
  (transduce
    (comp (take 100)
          (map #(.message %)))
    conj []
    (topic->reduceable "my_topic" kafka-config))
  ```
* `topic-onto-chan`

  Returns a `java.io.Closeable` consumer that posts all received
  messages onto a `clojure.core.async` channel.

See [the documentation][docs] for more details.

[docs]: https://ah45.github.io/clj-kafka-util/clj-kafka.consumer.util.html

## License

Copyright © 2015 Adam Harper.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

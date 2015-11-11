(ns clj-kafka.consumer.util
  (:require [clojure.core.async :as async]
            [clj-kafka.core :as kafka]
            [clj-kafka.consumer.zk :as kafka.consumer])
  (:import [kafka.serializer Decoder StringDecoder]
           [kafka.consumer ConsumerTimeoutException]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def default-read-all-timeout-ms
  "The default `consumer.timeout.ms` value used when creating Kafka
  consumers: 30 milliseconds."
  30)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn string-decoder
  "Returns a new `StringDecoder` instance."
  []
  (StringDecoder. nil))

(defn topic-onto-chan
  "Consumes from `topic` and posts received messages to channel `ch`.

  Returns a `java.io.Closeable` that, when `.close`d, shuts down the
  associated Kafka consumer and, if `close?` is `true` (the default)
  closes `ch`.

  Arguments:

  * `topic`, the name of the Kafka topic to consume.
  * `kafka-config`, a map of Kafka connection properties suitable for
    passing to `clj-kafka.consumer/consumer` to create a consumer.
  * `key-decoder`, an optional decoder to use for decoding the message
    keys. Will use `clj-kafka.consumer/default-decoder` if not
    specified.
  * `value-decoder`, an optional decoder to use for decoding the message
    values. Will use `clj-kafka.consumer/default-decoder` if not
    specified.
  * `close?`, a boolean dictating whether or not to close `ch` when we
    close. Defaults to `true`."
  ([ch topic kafka-config]
   (topic-onto-chan ch topic kafka-config nil nil true))
  ([ch topic kafka-config close?]
   (topic-onto-chan ch topic kafka-config nil nil close?))
  ([ch topic kafka-config ^Decoder key-decoder ^Decoder value-decoder]
   (topic-onto-chan ch topic kafka-config key-decoder value-decoder true))
  ([ch topic kafka-config ^Decoder key-decoder ^Decoder value-decoder close?]
   (let [kc (kafka.consumer/consumer kafka-config)
         stream (kafka.consumer/create-message-stream
                 kc topic
                 (or key-decoder (kafka.consumer/default-decoder))
                 (or value-decoder (kafka.consumer/default-decoder)))
         iter (.iterator stream)]
     (async/go-loop []
       (when (and (.hasNext iter) (async/>! ch (.next iter)))
         (recur)))
     (reify java.io.Closeable
       (close [this]
         (when close? (async/close! ch))
         (kafka.consumer/shutdown kc))))))

(defn topic->reduceable
  "Returns a reduceable collection that, when reduced, consumes all
  messages from the specified topic until no more arrive within
  `consumer.timeout.ms` of reading the last message (defaulting to
  `default-read-all-timeout-ms`.)

  If you've ever wanted to treat a Kafka topic as a
  reduceable/transducable collection then this is the function for
  you.

      ;; build map of messages from a topic
      (reduce
        (fn [rs m]
          (assoc rs (.key m) (.message m)))
        {}
        (topic->reduceable \"my_topic\" kafka-config))

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
          (topic->reduceable \"my_topic\" kafka-config)))
        ;; or transduction (simpler!)
        (transduce
          (take-while #(<= (.offset %) max-offset))
          conj []
          (topic->reduceable \"my_topic\" kafka-config))

      ;; get the content of the next 100 messages
      (transduce
        (comp (take 100)
              (map #(.message %)))
        conj []
        (topic->reduceable \"my_topic\" kafka-config))

  **Q:** How does this differ from running a transducer over the
  message stream?

  **A:** It provides two benefits:

  1. It gracefully handles reaching the “end” of the messages
     currently posted to the topic (if using a consumer timeout.)
  2. Abstracts away the creation of the consumer and stream so that
     you can easily substitute the topic for a collection and
     vice/versa just by replacing a single s-expression.

  Arguments:

  * `topic`, the name of the Kafka topic to consume.
  * `kafka-config`, a map of Kafka connection properties suitable for
    passing to `clj-kafka.consumer/consumer` to create a consumer.
  * `key-decoder`, an optional decoder to use for decoding the message
    keys. Will use `clj-kafka.consumer/default-decoder` if not
    specified.
  * `value-decoder`, an optional decoder to use for decoding the message
    values. Will use `clj-kafka.consumer/default-decoder` if not
    specified.

  **Important:** either the stream of messages from Kafka must _end_
  at some point or the reduction must reach a `reduced?` state;
  if not the reduction on the collection _will_ block forever and
  never return.

  By default the created consumer handles this by setting the
  `\"consumer.timeout.ms\"` property, to force the reduction to end
  when no new messages are received within a short period. Be aware
  that this may not work if you have a particularly fast moving topic.

  If you set the timeout yourself then, as a guideline, it should be
  less than the average time between message receipts for the topic.

  If you _want_ the reduction to run forever (e.g. when performing
  stream processing via transduction) then you will need to set the
  `\"consumer.timeout.ms\"` to `\"-1\"`.

  As with any other reduction you can stop the reduction at any time
  by returning a `reduced` value from your reducer function."
  ([topic kafka-config]
   (topic->reduceable topic kafka-config nil nil))
  ([topic kafka-config ^Decoder key-decoder ^Decoder value-decoder]
   (letfn [(consumer []
             (kafka.consumer/consumer
              (merge {"consumer.timeout.ms" (str default-read-all-timeout-ms)}
                     kafka-config)))
           (stream [consumer]
             (kafka.consumer/create-message-stream
              consumer topic
              (or key-decoder (kafka.consumer/default-decoder))
              (or value-decoder (kafka.consumer/default-decoder))))]
     (reify
       clojure.core.protocols/CollReduce
       (coll-reduce [coll f]
         (kafka/with-resource [c (consumer)] kafka.consumer/shutdown
           (let [iter (.iterator (stream c))
                 ret (atom nil)]
             (try
               (reset! ret (.next iter))
               (try
                 (loop [m (.next iter)]
                   (let [rs (f @ret m)]
                     (if (reduced? rs)
                       (reset! ret @rs)
                       (do (reset! ret rs)
                           (recur (.next iter))))))
                 (catch ConsumerTimeoutException e
                   @ret))
               (catch ConsumerTimeoutException e
                 (f))))))
       (coll-reduce [coll f val]
         (kafka/with-resource [c (consumer)] kafka.consumer/shutdown
           (let [iter (.iterator (stream c))
                 ret (atom val)]
             (try
               (loop [m (.next iter)]
                 (let [rs (f @ret m)]
                   (if (reduced? rs)
                     (reset! ret @rs)
                     (do (reset! ret rs)
                         (recur (.next iter))))))
               (catch ConsumerTimeoutException e
                 @ret)))))))))

(defn topic->map
  "Returns a map of all messages able to be read from the specified
  topic up to the point when no new messages arrive inside
  `consumer.timeout.ms` of reading the last message.

  This is a simple wrapper over `topic->reduceable` that builds a map
  of `{message-key message-value}`, returning a map of the last read
  values for each key."
  ([topic kafka-config]
   (topic->map topic kafka-config nil nil))
  ([topic kafka-config ^Decoder key-decoder ^Decoder value-decoder]
   (reduce
    (fn [rs m]
      (assoc rs (.key m) (.message m)))
    {}
    (topic->reduceable topic kafka-config key-decoder value-decoder))))

(defn read-all-from-topic
  "Returns a collection of all messages readable from the specified
  topic up to the point when no new messages arrive inside
  `consumer.timeout.ms` of reading the last message."
  ([topic kafka-config]
   (read-all-from-topic topic kafka-config nil nil))
  ([topic kafka-config ^Decoder key-decoder ^Decoder value-decoder]
   (reduce
    conj []
    (topic->reduceable topic kafka-config key-decoder value-decoder))))

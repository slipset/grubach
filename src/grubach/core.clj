(ns grubach.core
  (:require
   [clj-kafka.consumer.simple :as kc]
   [clj-kafka.core :as kafka])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequest FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.javaapi OffsetRequest TopicMetadataRequest FetchResponse]
           [kafka.common TopicAndPartition]))

(def kafka-consumer (kc/consumer "kafka1.di.telenordigital.com" 9092 "lol"))

(defn partition-metadata [consumer topic]
  (->> [topic]
       (kc/topic-meta-data kafka-consumer)
       first
       :partition-metadata
       first))

(defn leader-consumer [consumer topic]
  (let [{:keys [leader]} (partition-metadata consumer topic)]
    (kc/consumer (:host leader) (:port leader) "lol")))

(defn offset [consumer topic kw]
  (with-open [lc (leader-consumer consumer topic)]
    (kc/topic-offset lc topic 0 kw)))

(defn message-at [consumer topic offset]
  (let [req (kc/fetch-request "lol" topic 0 offset 1 :max-wait 30000)
        {:keys [leader]} (partition-metadata consumer topic)]
    (-> (.fetch consumer req)
        (.messageSet  topic 0)
        .iterator
        iterator-seq
        first
        kafka/to-clojure)))


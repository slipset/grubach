(ns grubach.core
  (:require
   [clj-kafka.consumer.simple :as kc]
   [clj-kafka.core :as kafka]
   [cheshire.core :as json])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequest FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.javaapi OffsetRequest TopicMetadataRequest FetchResponse]
           [kafka.common TopicAndPartition]))

(def kafka-consumer (kc/consumer "kafka1.di.telenordigital.com" 9092 "grubach"))

(defn partition-metadata [consumer topic]
  (->> [topic]
       (kc/topic-meta-data kafka-consumer)
       first
       :partition-metadata
       first))

(defn leader-consumer [consumer topic]
  (let [{:keys [leader]} (partition-metadata consumer topic)]
    (kc/consumer (:host leader) (:port leader) "grubach")))

(defn offset [consumer topic kw]
  (with-open [lc (leader-consumer consumer topic)]
    (kc/topic-offset lc topic 0 kw)))

(defn as-edn [message]
  (-> message
      kafka/to-clojure
      (update  :value (fn [x] (json/decode (String. (byte-array x)) true)))))

(defn message-at
  ([consumer topic offset] (message-at consumer topic offset as-edn))
  ([consumer topic offset deserializer]
   (let [req (kc/fetch-request "grubach" topic 0 offset 1000000 :max-wait 30000)]
     (with-open [lc (leader-consumer consumer topic)]
       (-> (.fetch lc req)
           (.messageSet  topic 0)
           .iterator
           iterator-seq
           first
           deserializer)))))

(defn topics [consumer]
  (let [req (TopicMetadataRequest. [])
        resp (.send consumer req)]
    (sort (map #(.topic %) (.topicsMetadata resp)))))

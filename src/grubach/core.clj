(ns grubach.core
  (:require
   [clj-kafka.consumer.simple :as kc]
   [clj-kafka.core :as kafka]
   [cheshire.core :as json])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequest FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.javaapi OffsetRequest TopicMetadataRequest FetchResponse]
           [kafka.common TopicAndPartition]))

(def client-id "grubach")

(defn consumer
  "Create a consumer to connect to host and port. Port will
   normally be 9092."
  ([{:keys [host port timeout buffersize] :or {timeout 100000 buffer-size 100000}}]
   (consumer host port timeout buffersize))
  ([host ^Long port & {:keys [^Long timeout ^Long buffer-size] :or
                       {timeout 100000 buffer-size 100000}}]
   (SimpleConsumer. host
                    (Integer/valueOf port)
                    (Integer/valueOf timeout)
                    (Integer/valueOf buffer-size)
                    client-id)))

(def kafka-consumer (consumer "kafka1.di.telenordigital.com" 9092 ))

(defn topic-meta-data
  ([consumer topics] (topic-meta-data consumer topics kafka/to-clojure))
  ([consumer topics deserializer]
   (->> topics
        TopicMetadataRequest.
        (.send consumer)
        deserializer)))

(defn partition-metadata [consumer & topics]
  (->> topics
       (topic-meta-data consumer)
       first
       :partition-metadata
       first))

(defn leader-consumer [c topic]
  (let [{:keys [leader] :as part-metadata} (partition-metadata c topic)]
    (consumer leader)))

(defn with-leader [f consumer topic & args]
  (with-open [lc (leader-consumer consumer topic)]
    (apply f lc topic args)))

(def earliest -2)
(def latest -1)

(defn offset* [consumer topic at]
  (let [hm (java.util.HashMap. {(TopicAndPartition. topic 0)
                                (PartitionOffsetRequestInfo. at 1)})]
    (-> (.getOffsetsBefore consumer (OffsetRequest. hm
                                                    (kafka.api.OffsetRequest/CurrentVersion)
                                                    client-id))
        (.offsets topic 0)
        first)))

(defn offset [consumer topic kw]
  (with-leader offset* consumer topic kw))

(defn as-edn [message]
  (-> message
      kafka/to-clojure
      (update  :value (fn [x] (json/decode (String. (byte-array x)) true)))))

(defn message-at*
  ([consumer topic offset] (message-at consumer topic offset as-edn))
  ([consumer topic offset deserializer]
   (-> (.fetch consumer (kc/fetch-request client-id topic 0 offset 1000000 :max-wait 300000))
       (.messageSet  topic 0)
       .iterator
       iterator-seq
       first
       deserializer)))

(defn message-at
  ([consumer topic offset] (message-at consumer topic offset as-edn))
  ([consumer topic offset deserializer]
   (with-leader message-at* consumer topic offset deserializer)))

(defn last-message*
  ([consumer topic] (last-message* consumer topic as-edn))
  ([consumer topic deserializer]
   (message-at consumer topic (offset consumer topic latest) deserializer)))

(defn last-message
  ([consumer topic ] (last-message consumer topic as-edn))
  ([consumer topic deserializer]
   (with-leader last-message* consumer topic deserializer)))

(defn topics [consumer]
  (->> (topic-meta-data consumer [] identity)
       .topicsMetadata
       (map #(.topic %))
       sort))

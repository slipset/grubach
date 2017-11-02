(ns grubach.core
  (:refer-clojure :exclude [filter map])
  (:require
   [clj-kafka.core :as kafka]
   [cheshire.core :as json]
   [clj-time.core :as t]
   [clj-time.coerce :as c])
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
      (update  :value (fn [x] (json/parse-stream
                               (java.io.InputStreamReader.
                                (java.io.ByteArrayInputStream. (byte-array x))) true)))))

(defn fetch-request
  [client-id topic ^Long partition offset fetch-size & {:keys [max-wait min-bytes]}]
  (.build (doto (FetchRequestBuilder. )
            (.clientId client-id)
            (.addFetch topic (Integer/valueOf partition) offset fetch-size)
            (#(when max-wait (.maxWait % max-wait)))
            (#(when min-bytes (.minBytes % min-bytes))))))

(defn message-at*
  ([consumer topic offset] (message-at* consumer topic offset as-edn))
  ([consumer topic offset deserializer]
   (-> (.fetch consumer (fetch-request client-id topic 0 offset 1000000 :max-wait 300000))
       (.messageSet  topic 0)
       .iterator
       iterator-seq
       first
       deserializer)))

(defn message-at
  ([consumer topic offset] (message-at consumer topic offset as-edn))
  ([consumer topic offset deserializer]
   (with-leader message-at* consumer topic offset deserializer)))

(defn message-from*
  ([consumer topic offset] (message-from* consumer topic offset as-edn))
  ([consumer topic offset deserializer]
   (clojure.core/map deserializer (-> (.fetch consumer (fetch-request client-id topic 0 offset 100000000 :max-wait 300000))
                         (.messageSet  topic 0)
                         .iterator
                         iterator-seq))))

(defn message-from
  ([consumer topic offset] (message-from consumer topic offset as-edn))
    ([consumer topic offset deserializer]
   (with-leader message-from* consumer topic offset deserializer)))

(defn last-message*
  ([consumer topic] (last-message* consumer topic as-edn))
  ([consumer topic deserializer]
   (message-at consumer topic (dec (offset consumer topic latest)) deserializer)))

(defn last-message
  ([consumer topic ] (last-message consumer topic as-edn))
  ([consumer topic deserializer]
   (with-leader last-message* consumer topic deserializer)))

(defn topics [consumer]
  (->> (topic-meta-data consumer [] identity)
       .topicsMetadata
       (map #(.topic %))
       sort))

(defn bisect [consumer topic comparator lower upper]
  (let [offset (/ (+ lower upper) 2)
        {:keys [offset value] :as event} (message-at consumer topic offset)
        compared (comparator value)]
    (println "Offset:" offset)
    (cond (= compared 0) event
          (pos? compared)  (bisect consumer topic comparator offset upper)
          (neg? compared) (bisect consumer topic comparator lower offset))))

(defn filter [consumer topic p lower upper]
  (->> (range lower upper)
       (clojure.core/map (partial message-at consumer topic))
       (clojure.core/filter p)))

(defn map [consumer topic f offset]
  (clojure.core/map f (message-from consumer topic offset)))

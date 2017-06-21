(ns grubach.core
  (:require
    [clj-kafka.consumer.simple :as kc])
  (:import [kafka.javaapi.consumer SimpleConsumer]
           [kafka.api FetchRequest FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.javaapi OffsetRequest TopicMetadataRequest FetchResponse]
           [kafka.common TopicAndPartition]))

(def config {"zookeeper.connect" "zk1.int.di.telenordigital.com:2181"
             "group.id" "grubach.consumer"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "false"})




(def kafka-consumer (kc/consumer "kafka1.di.telenordigital.com" 9092 ""))

(defn partition-metadata [consumer topic]
  (->> [topic]
       (kc/topic-meta-data kafka-consumer)
       first
       :partition-metadata
       first))

(partition-metadata kafka-consumer "fraud-detector-production-dob-event-processed-successful")

(defn offset [consumer topic kw]
  (let [{:keys [leader]} (partition-metadata consumer topic)]
    (with-open [leader-consumer (kc/consumer (:host leader) (:port leader) "")]
      (kc/topic-offset leader-consumer topic 0 kw))))



(kafka/with-resource [c (zk/consumer config)]
  zk/shutdown
  (println c)
  (doall (take 1 (zk/messages c "payment2-staging-charge-started"))))

(def consumer (simple/consumer "kafka1.di.telenordigital.com" 9092
                               "gruback-simple"))

(simple/topic-meta-data consumer ["xddi-payment2-production-charge-started"])

(with-open [zk (admin/zk-client "zk1.int.di.telenordigital.com:2181")]
  (admin/topic-exists? zk "xddi-payment2-production-charge-started"))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))



(.partitionsFor consumer "xddi-payment2-production-charge-started")


(def config {:bootstrap.servers ["kafka1.di.telenordigital.com:9092"]
             :group.id "grubach"
             :auto.offset.reset :earliest})
(def  first-topic-partition {:topic "payment2-production-charge-started"
                             :partition 0})

(def consumer  (consumer/make-consumer config
                                        (deserializers/keyword-deserializer)
                                        (deserializers/edn-deserializer)))


(println consumer)
(assign-partitions! consumer [first-topic-partition])
(seek-to-offset! consumer first-topic-partition 8179317)
(println "Next offset1:" (next-offset consumer first-topic-partition))
(seek-to-end-offset! consumer [first-topic-partition])
(def foo (poll! consumer))
(println "Next offset:" (next-offset c first-topic-partition))



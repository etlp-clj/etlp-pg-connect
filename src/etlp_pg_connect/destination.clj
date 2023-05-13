(ns etlp-pg-connect.destination
  (:require   [clojure.string :as str]
              [clojure.tools.logging :as log]
              [etlp-pg-connect.utils :refer [create-connection create-pg-destination]]
              [etlp.connector.protocols :refer [EtlpDestination]]
              [etlp.connector.dag :as dag])
  (:gen-class))

(defn pg-destination-topology [{:keys [processors db]}]
  (let [db-conn    (create-connection (db :config))
        db-sink    (create-pg-destination db-conn db)
        threads    (db :threads)
        partitions (db :partitions)
        entities   {:etlp-input {:channel (a/chan (a/buffer partitions))
                                 :meta    {:entity-type :processor
                                           :processor   (processors :etlp-processor)}}

                    :etlp-output {:meta {:entity-type :xform-provider
                                         :threads     threads
                                         :partitions  partitions
                                         :xform       (comp
                                                       (partition-all partitions)
                                                       (map @db-sink)
                                                       (keep (fn [l] (println "Record created :: " (get (first l) :id)))))}}}
        workflow [[:etlp-input :etlp-output]]]

    {:entities entities
     :workflow workflow}))

(defrecord EtlpPostgresDestination [db processors topology-builder]
  EtlpDestination
  (write![this]
    (let [topology     (topology-builder this)
          etlp-inst         (dag/build topology)]

      etlp-inst)))

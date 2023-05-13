(ns etlp-pg-connect.source
  (:require   [clojure.string :as str]
              [clojure.tools.logging :as log]
              [etlp.connector.protocols :refer [EtlpSource]]
              [clj-postgresql.core :as pg]
              [etlp.connector.dag :as dag]
              [clojure.core.async :as a :refer [<! >! <!! >!! go-loop chan close! timeout]]
              [clojure.java.jdbc :as jdbc])
  (:import [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))

(defprotocol PaginatedJDBCResource
  (execute [this])
  (total [this])
  (pageSize [this])
  (pollInterval [this]))

(defrecord JDBCResource [db-spec query page-size poll-interval offset-atom]
  PaginatedJDBCResource
  (execute [this]
    (let [page-size (:page-size this)
          offset-atom @(:offset-atom this)
          table-name (->> (re-find #"(?i)FROM (\w+)" query) second keyword)
          total (.total this)
          select-query (str "(" query " ORDER BY created_at LIMIT " page-size " OFFSET " offset-atom ")")
          sql (str "SELECT json_build_object(
                        'total', ", total, ",
                        'count', count(q.*),
                        'offset', ", offset-atom, ",
                        'results', json_agg(row_to_json(q))) FROM (" select-query ") AS q")
          results (jdbc/query db-spec sql)]
      results))
  (total [this]
    (let [db-spec (:db-spec this)
          query (:query this)
          sql (str "SELECT COUNT(*) FROM (" query ") _")]
      (-> (jdbc/query db-spec sql)
          first
          (get :count))))
  (pageSize [this]
    (:page-size this))
  (pollInterval [this]
    (:poll-interval this)))

(defn start [resource]
  (let [result-chan (a/chan)]
    (go-loop [offset (:offset-atom resource)]
      (let [page (.execute resource)]
        (a/>! result-chan (first page))
        (let [total (get-in (first page) [:json_build_object "total"])
              count (get-in (first page) [:json_build_object "count"])
              new-offset (+ @offset (.pageSize resource))]
          (when (< new-offset total)
            (Thread/sleep (.pollInterval resource))
            (reset! offset new-offset)
            (recur offset)))))
    result-chan))

(def create-jdbc-processor! (fn [opts]
                              (let [processor (map->JDBCResource opts)]
                                processor)))

(defn list-pg-processor [data]
  (let [opts (data :db-config)
        jdbc-reader (create-jdbc-processor! opts)
        results (start jdbc-reader)]
    results))

(def get-pg-rows (fn [data]
                   (let [reducer (data :reducer)
                         output  (a/chan (a/buffer 2000000) (mapcat reducer))]
                        output)))

(def etlp-processor (fn [data]
                      (if (instance? ManyToManyChannel data)
                        data
                        (data :channel))))

(defn pg-process-topology [{:keys [db-config processors reducers reducer]}]
  (let [entities {:list-pg-processor {:db-config db-config
                                      :channel   (a/chan (a/buffer (db-config :page-size)))
                                      :meta      {:entity-type :processor
                                                  :processor   (processors :list-pg-processor)}}
                  :xform-processor {:meta {:entity-type :xform-provider
                                           :xform (reducers reducer)}}
                  :etlp-output {:channel (a/chan (a/buffer (db-config :page-size)))
                                :meta    {:entity-type :processor
                                          :processor   (processors :etlp-processor)}}}
        workflow [[:list-pg-processor :xform-processor]
                  [:xform-processor :etlp-output]]]
    {:entities entities
     :workflow workflow}))

(defrecord EtlpPostgresSource [db-config processors topology-builder reducers reducer]
  EtlpSource
  (spec [this] {:supported-destination-streams []
                :supported-source-streams      [{:stream_name "pg_stream"
                                                 :schema      {:type       "object"
                                                               :properties {:db-config  {:type        "object"
                                                                                         :description "S3 connection configuration."}

                                                                            :processors {:type        "object"
                                                                                         :description "Processors to be used to extract and transform data from the S3 bucket."}}}}]})

  (check [this]
    (let [errors (conj [] (when (nil? (:db-config this))
                            "s3-config is missing")
                       (when (nil? (:reducers this))
                         "bucket is missing")
                       (when (nil? (:processors this))
                         "processors is missing"))]
      {:status  (if (empty? errors) :valid :invalid)
       :message (if (empty? errors) "Source configuration is valid."
                    (str "Source configuration is invalid. Errors: " (clojure.string/join ", " errors)))}))

  (discover [this]
            ;; TODO use config and topology to discover schema from mappings
    {:streams [{:stream_name "pg_stream"
                :schema      {:type       "object"
                              :properties {:data {:type "string"}}}}]})
  (read! [this]
    (let [topology     (topology-builder this)
          workflow         (dag/build topology)]

     workflow)))

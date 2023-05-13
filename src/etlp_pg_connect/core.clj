(ns etlp-pg-connect.core
  (:require   [clojure.string :as str]
              [clojure.tools.logging :as log]
              [etlp-pg-connect.source :refer (map->EtlpPostgresSource)]
              [etlp-pg-connect.destination :refer (map->EtlpPostgresDestination)])
  (:gen-class))

(def create-postgres-source! (fn [{:keys [db-config reducers reducer] :as opts}]
                               (let [pg-connector (map->EtlpAirbytePostgresSource {:db-config        db-config
                                                                                   :processors       {:list-pg-processor list-pg-processor
                                                                                                      :read-pg-chunks    get-pg-rows
                                                                                                      :etlp-processor    etlp-processor}
                                                                                   :reducers         reducers
                                                                                   :reducer          reducer
                                                                                   :topology-builder pg-process-topology})]
                                 pg-connector)))


(def create-postgres-destination! (fn [{:keys [pg-config table specs threads partitions] :as opts}]
                                    (let [pg-destination (map->EtlpPostgresDestination {:db  {:config pg-config
                                                                                              :table table
                                                                                              :specs specs
                                                                                              :threads threads
                                                                                              :partitions partitions}
                                                                                        :processors
                                                                                        {:etlp-processor   etlp-processor}
                                                                                        :topology-builder pg-destination-topology})]
                                     pg-destination)))

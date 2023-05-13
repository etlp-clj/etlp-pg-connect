(defproject etlp-pg-connect "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [cheshire "5.10.0"]
                 [clj-postgresql "0.7.0"]
                 [com.github.argee/etlp "0.4.1-SNAPSHOT"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojure/java.jdbc "0.7.11"]]
  :repl-options {:init-ns etlp-pg-connect.core})

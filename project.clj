(defproject org.clojars.ah45/clj-kafka-util "0.2.0-SNAPSHOT"
  :description "A collection of utility fns related to clj-kafka"
  :url "https://github.com/ah45/clj-kafka-util"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.371"]
                 [clj-kafka "0.3.2"]]
  :plugins [[lein-codox "0.9.0"]]
  :codox {:source-uri "https://github.com/ah45/clj-kafka-util/blob/master/{filepath}#L{line}"
          :output-path "./"
          :metadata {:doc/format :markdown}})

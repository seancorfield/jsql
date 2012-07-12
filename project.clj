(defproject jsql "0.1.0-SNAPSHOT"
  :description "Experimental DSL for SQL generation for next version of clojure.java.jdbc."
  :url "https://github.com/seancorfield/jsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-autoexpect "0.1.1"]
            [lein-expectations "0.0.5"]
            [lein-swank "1.4.4"]]
  :profiles {:dev {:dependencies [[expectations "1.4.3"]
                                  [clojure-source "1.3.0"]
                                  [mysql/mysql-connector-java "5.1.6"]]}}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.2.3"]])

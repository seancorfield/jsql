(defproject jsql "0.1.0-SNAPSHOT"
  :description "Experimental DSL for SQL generation for next version of clojure.java.jdbc."
  :url "https://github.com/seancorfield/jsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  ;; currently relying on experimental as-identifier functionality in clojure.java.jdbc
  :repositories [["sonatype-snapshots" "https://oss.sonatype.org/content/repositories/snapshots/"]]
  :dev-dependencies [[lein-autoexpect "0.1.1"]
                     [lein-expectations "0.0.3"]
                     [expectations "1.4.3"]
                     [clojure-source "1.3.0"]
                     [swank-clojure "1.4.2"]
                     [mysql/mysql-connector-java "5.1.6"]]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/java.jdbc "0.2.3-SNAPSHOT"]])

(ns jsql.test.db-ops
  (:require [jsql.core :refer :all]
            [clojure.java.jdbc :as jdbc]
            [expectations.scenarios :refer [scenario expect]]))

;; database connections used for testing:

(def mysql-db {:subprotocol "mysql"
               :subname "//127.0.0.1:3306/clojure_test"
               :user "clojure_test"
               :password "clojure_test"})

;; We start with all tables dropped and each test has to create the tables
;; necessary for it to do its job, and populate it as needed...

(defn- create-test-table
  "Create a standard test table. Must be inside with-connection.
   For MySQL, ensure table uses an engine that supports transactions!"
  [table db]
  (let [p (:subprotocol db)]
    (jdbc/create-table
      table
      [:id :int (if (= "mysql" p) "PRIMARY KEY AUTO_INCREMENT" "DEFAULT 0")]
      [:name "VARCHAR(32)" (if (= "mysql" p) "" "PRIMARY KEY")]
      [:appearance "VARCHAR(32)"]
      [:cost :int]
      [:grade :real]
      :table-spec (if (or (= "mysql" p) (and (string? db) (re-find #"mysql:" db)))
                    "ENGINE=InnoDB" ""))))

(defn- drop-test-table
  "Drop a specific test table (if defined). Must be inside with-connection."
  [table db]
  (try
    (jdbc/drop-table table)
    (catch Exception e
      ;; ignore failure
      )))

(scenario ;; select with no results is empty sequence
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [result (query mysql-db
                     (select * :fruit))]
   (expect [] result)))

(scenario ;; insert! one row returns new key
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [result (insert! mysql-db :fruit
                       {:name "Apple"})]
   (expect {:generated_key 1} result)))

(scenario ;; insert! and query returns inserted row
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [new-key (insert! mysql-db :fruit
                        {:name "Apple"})
       rows (query mysql-db
                   (select * :fruit))]
   (expect {:generated_key 1} new-key)
   (expect [{:id 1 :name "Apple" :appearance nil :grade nil :cost nil}] rows)))

(scenario ;; insert! and query returns inserted row
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [new-key (insert! mysql-db :fruit
                        [:name] ["Apple"])
       rows (query mysql-db
                   (select * :fruit))]
   (expect {:generated_key 1} new-key)
   (expect [{:id 1 :name "Apple" :appearance nil :grade nil :cost nil}] rows)))

(scenario ;; insert! two records and query returns inserted rows
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [new-key (insert! mysql-db :fruit
                        {:name "Apple"}
                        {:name "Pear"})
       rows (query mysql-db
                   (select * :fruit
                           (order-by :id)))]
   (expect [{:generated_key 1}
            {:generated_key 2}] new-key)
   (expect [{:id 1 :name "Apple" :appearance nil :grade nil :cost nil}
            {:id 2 :name "Pear" :appearance nil :grade nil :cost nil}] rows)))

(scenario ;; insert! two records and query returns inserted rows
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [update-counts (insert! mysql-db :fruit
                              [:name] ["Apple"] ["Pear"])
       rows (query mysql-db
                   (select * :fruit
                           (order-by :id)))]
   (expect [1 1] update-counts)
   (expect [{:id 1 :name "Apple" :appearance nil :grade nil :cost nil}
            {:id 2 :name "Pear" :appearance nil :grade nil :cost nil}] rows)))

(scenario ;; insert! update! and query returns updated row
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [new-key (insert! mysql-db :fruit
                        {:name "Apple"})
       update-result (update! mysql-db :fruit
                              {:cost 12 :grade 1.2 :appearance "Green"}
                              (where {:id 1}))
       rows (query mysql-db
                   (select * :fruit))]
   (expect {:generated_key 1} new-key)
   (expect [1] update-result)
   (expect [{:id 1 :name "Apple" :appearance "Green" :grade 1.2 :cost 12}] rows)))

(scenario ;; insert! delete! and query returns no rows
 (jdbc/with-connection mysql-db
   (drop-test-table :fruit mysql-db)
   (create-test-table :fruit mysql-db))
 (let [new-key (insert! mysql-db :fruit
                        {:name "Apple"})
       delete-result (delete! mysql-db :fruit
                              (where {:id 1}))
       rows (query mysql-db
                   (select * :fruit))]
   (expect {:generated_key 1} new-key)
   (expect [1] delete-result)
   (expect [] rows)))

(scenario ;; error conditions for bad argument combinations
 (expect IllegalArgumentException (insert! mysql-db))
 (expect IllegalArgumentException (insert! mysql-db :entities as-is))
 (expect IllegalArgumentException (insert! mysql-db {:name "Apple"} [:name]))
 (expect IllegalArgumentException (insert! mysql-db {:name "Apple"} [:name] :entities as-is))
 (expect IllegalArgumentException (insert! mysql-db [:name]))
 (expect IllegalArgumentException (insert! mysql-db [:name] :entities as-is)))
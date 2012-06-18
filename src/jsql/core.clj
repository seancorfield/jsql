(ns jsql.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string
             :refer [join lower-case]
             :rename {join str-join
                      lower-case str-lower}]
            [clojure.walk :refer [postwalk]]))

(defn- col-str [col entities]
  (if (map? col)
    (let [[k v] (first col)]
      (str (jdbc/as-identifier k entities) " AS " (jdbc/as-identifier v entities)))
    (jdbc/as-identifier col entities)))

(defn- table-str [table entities]
  (if (map? table)
    (let [[k v] (first table)]
      (str (jdbc/as-identifier k entities) " " (jdbc/as-identifier v entities)))
    (jdbc/as-identifier table entities)))

(def ^:private entity-symbols
  #{"delete" "delete!"
    "insert" "insert!"
    "select" "join" "where" "order-by"
    "update" "update!"})

(defmacro entities [entities sql]
  (postwalk (fn [form]
              (if (and (seq? form)
                       (symbol? (first form))
                       (entity-symbols (name (first form))))
                (concat form [:entities entities])
                form)) sql))

(def ^:private identifier-symbols
  #{"query"})

(defmacro identifiers [identifiers sql]
  (postwalk (fn [form]
              (if (and (seq? form)
                       (symbol? (first form))
                       (identifier-symbols (name (first form))))
                (concat form [:identifiers identifiers])
                form)) sql))

(def as-is identity)
(def lower-case str-lower)
(defn quoted [q] (partial jdbc/as-quoted-str q))

(defn delete [table [where & params] & {:keys [entities] :or {entities as-is}}]
  (into [(str "DELETE FROM " (table-str table entities)
              (when where " WHERE ") where)]
        params))

(defn insert [tables & clauses]
  [""])

(defn join [table on-map & {:keys [entities] :or {entities as-is}}]
  (str "JOIN " (table-str table entities) " ON "
       (str-join
        " AND "
        (map (fn [[k v]] (str (jdbc/as-identifier k entities) " = " (jdbc/as-identifier v entities))) on-map))))

(defn- order-direction [col entities]
  (if (map? col)
    (str (jdbc/as-identifier (first (keys col)) entities)
         " "
         (let [dir (first (vals col))]
           (get {:asc "ASC" :desc "DESC"} dir dir)))
    (str (jdbc/as-identifier col entities) " ASC")))

(defn order-by [cols & {:keys [entities] :or {entities as-is}}]
  (str "ORDER BY "
       (if (or (string? cols) (keyword? cols) (map? cols))
         (order-direction cols entities)
         (str-join "," (map #(order-direction % entities) cols)))))

(defn select [col-seq table & clauses]
  (let [joins (take-while string? clauses)
        where-etc (drop (count joins) clauses)
        [where-clause & more] where-etc
        [where & params] (when-not (keyword? where-clause) where-clause)
        order-etc (if (keyword? where-clause) where-etc more)
        [order-clause & more] order-etc
        order-by (when (string? order-clause) order-clause)
        options (if order-by more order-etc)
        {:keys [entities] :or {entities as-is}} (apply hash-map  options)]
    (cons (str "SELECT "
               (cond
                (= * col-seq) "*"
                (or (string? col-seq)
                    (keyword? col-seq)
                    (map? col-seq)) (col-str col-seq entities)
                    :else (str-join "," (map #(col-str % entities) col-seq)))
               " FROM " (table-str table entities)
               (when (seq joins) (str-join " " (cons "" joins)))
               (when where " WHERE ")
               where
               (when order-by " ")
               order-by)
          params)))

(defn update [table set-map & where-etc]
  (let [[where-clause & options] (when-not (keyword? (first where-etc)) where-etc)
        [where & params] where-clause
        {:keys [entities] :or {entities as-is}} (if (keyword? (first where-etc)) where-etc options)
        ks (keys set-map)
        vs (vals set-map)]
    (cons (str "UPDATE " (table-str table entities)
               " SET " (str-join
                        ","
                        (map (fn [k v]
                               (str (jdbc/as-identifier k entities)
                                    " = "
                                    (if (nil? v) "NULL" "?")))
                             ks vs))
               (when where " WHERE ")
               where)
          (concat (remove nil? vs) params))))

(defn where [param-map & {:keys [entities] :or {entities as-is}}]
  (let [ks (keys param-map)
        vs (vals param-map)]
    (cons (str-join
           " AND "
           (map (fn [k v]
                  (str (jdbc/as-identifier k entities)
                       (if (nil? v) " IS NULL" " = ?")))
                ks vs))
          (remove nil? vs))))

(defn query [db sql-params & {:keys [result-set row identifiers]
                              :or {result-set doall row identity identifiers lower-case}}]
  (jdbc/with-connection db
    (jdbc/with-query-results rs
      (vec sql-params)
      (result-set (map row rs)))))

(defn execute! [db sql-params]
  (jdbc/with-connection db
    (jdbc/do-prepared (first sql-params) (rest sql-params))))

(defn delete! [db table where-map & {:keys [entities]
                                     :or {entities as-is}}]
  (execute! db (delete table where-map :entities entities)))

(defn insert! [db table & maps-or-cols-and-values-etc]
  (let [records (take-while map? maps-or-cols-and-values-etc)
        cols-and-values-etc (drop (count records) maps-or-cols-and-values-etc)
        cols-and-values (take-while (comp not keyword?) cols-and-values-etc)
        [cols & values] cols-and-values
        options (drop (count cols-and-values) cols-and-values-etc)
        {:keys [entities identifiers]
         :or {entities as-is identifiers lower-case}} options]
    (if cols
      (do
        (when (seq records) (throw (IllegalArgumentException. "insert! may take records or columns and values, not both")))
        (when (nil? values) (throw (IllegalArgumentException. "insert! called with columns but no values"))))
      (when (empty? records) (throw (IllegalArgumentException. "insert! called without data to insert"))))
    (jdbc/with-connection db
      (condp = (count records)
        0 (apply jdbc/insert-values table cols values)
        1 (jdbc/insert-record table (first records))
        (apply jdbc/insert-records table records)))))

(defn update! [db table set-map where-map & {:keys [entities]
                                             :or {entities as-is}}]
  (execute! db (update table set-map where-map :entities entities)))
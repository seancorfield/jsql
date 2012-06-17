(ns jsql.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join] :rename {join str-join}]
            [clojure.walk :refer [postwalk]]))

(defn- col-str [col entity]
  (if (map? col)
    (let [[k v] (first col)]
      (str (jdbc/as-identifier k entity) " AS " (jdbc/as-identifier v entity)))
    (jdbc/as-identifier col entity)))

(defn- table-str [table entity]
  (if (map? table)
    (let [[k v] (first table)]
      (str (jdbc/as-identifier k entity) " " (jdbc/as-identifier v entity)))
    (jdbc/as-identifier table entity)))

(def ^:private sql-syms
  #{"delete" "insert" "join" "select" "update" "where"})

(defmacro naming [entity sql]
  (postwalk (fn [form]
              (if (and (seq? form)
                       (symbol? (first form))
                       (sql-syms (name (first form))))
                (concat form [:entity entity])
                form)) sql))

(defn delete [table [where & params] & {:keys [entity] :or {entity identity}}]
  (into [(str "DELETE FROM " (table-str table entity)
              (when where " WHERE ") where)]
        params))

(defn insert [tables & clauses]
  [""])

(defn join [table on-map & {:keys [entity] :or {entity identity}}]
  (str " JOIN " (table-str table entity) " ON "
       (str-join
        " AND "
        (map (fn [[k v]] (str (jdbc/as-identifier k entity) " = " (jdbc/as-identifier v entity))) on-map))))

(defn select [col-seq table & clauses]
  (let [joins (take-while string? clauses)
        where-etc (drop (count joins) clauses)
        [where & params] (when-not (keyword? (first where-etc))
                           (first where-etc))
        options (if where (rest where-etc) where-etc)
        {:keys [entity] :or {entity identity}} (apply hash-map  options)]
    (into [(str
            "SELECT "
            (cond
             (= * col-seq) "*"
             (or (string? col-seq)
                 (keyword? col-seq)
                 (map? col-seq)) (col-str col-seq entity)
             :else (str-join "," (map #(col-str % entity) col-seq)))
            " FROM " (table-str table entity)
            (str-join " " joins)
            (when where " WHERE ")
            where)]
          params)))

(defn update [table set-map & where-etc]
  (let [[where-clause & options] (when-not (keyword? (first where-etc)) where-etc)
        [where & params] where-clause
        {:keys [entity] :or {entity identity}} (if (keyword? (first where-etc)) where-etc options)
        ks (keys set-map)
        vs (vals set-map)]
    (into [(str "UPDATE " (table-str table entity)
                " SET " (str-join
                         ","
                         (map (fn [k v] (str (jdbc/as-identifier k entity)
                                            " = "
                                            (if (nil? v) "NULL" "?")))
                              ks vs))
                (when where " WHERE ")
                where
                )]
          (concat (remove nil? vs) params))))

(defn where [param-map & {:keys [entity] :or {entity identity}}]
  (let [ks (keys param-map)
        vs (vals param-map)]
    (cons
     (str-join
      " AND "
      (map (fn [k v]
             (str (jdbc/as-identifier k entity) (if (nil? v) " IS NULL" " = ?")))
           ks vs))
     (remove nil? vs))))

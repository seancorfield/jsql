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

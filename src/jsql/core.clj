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

(defn- insert-multi-row [table columns values entities]
  (let [nc (count columns)
        vcs (map count values)]
    (if (not (apply = nc vcs))
      (throw (IllegalArgumentException. "insert called with inconsistent number of columns / values"))
      (into [(str "INSERT INTO " (table-str table entities) " ( "
                  (str-join ", " (map (fn [col] (col-str col entities)) columns))
                  " ) VALUES "
                  (str-join ", "
                            (repeat (count values)
                                    (str "( "
                                         (str-join ", " (repeat nc "?"))
                                         " )"))))]
            (apply concat values)))))

(defn- insert-single-row [table row entities]
  (let [ks (keys row)]
    (into [(str "INSERT INTO " (table-str table entities) " ( "
                (str-join ", " (map (fn [col] (col-str col entities)) ks))
                " ) VALUES ( "
                (str-join ", " (repeat (count ks) "?"))
                " )")]
          (vals row))))

(defn insert [table & clauses]
  (let [rows (take-while map? clauses)
        n-rows (count rows)
        cols-and-vals-etc (drop n-rows clauses)
        cols-and-vals (take-while (comp not keyword?) cols-and-vals-etc)
        n-cols-and-vals (count cols-and-vals)
        no-cols-and-vals (zero? n-cols-and-vals)
        options (drop (+ (count rows) (count cols-and-vals)) clauses)
        {:keys [entities] :or {entities as-is}} (apply hash-map options)]
    (if (zero? n-rows)
      (if no-cols-and-vals
        (throw (IllegalArgumentException. "insert called without data to insert"))
        (if (< n-cols-and-vals 2)
          (throw (IllegalArgumentException. "insert called with columns but no values"))
          (insert-multi-row table (first cols-and-vals) (rest cols-and-vals) entities)))
      (if no-cols-and-vals
        (map (fn [row] (insert-single-row table row entities)) rows)
        (throw (IllegalArgumentException. "insert may take records or columns and values, not both"))))))

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
  (let [stmts (apply insert table maps-or-cols-and-values-etc)]
    (jdbc/with-connection db
      (if (string? (first stmts))
        (jdbc/do-prepared (first stmts) (rest stmts))
        (doall (map (fn [row] (first (vals (jdbc/do-prepared-return-keys
                                           (first row) (rest row)))))
                    stmts))))))

(defn update! [db table set-map where-map & {:keys [entities]
                                             :or {entities as-is}}]
  (execute! db (update table set-map where-map :entities entities)))
(ns jsql.test.core
  (:require [jsql.core :refer :all]
            [clojure.java.jdbc :as jdbc]
            [expectations :refer [expect]]))

(expect ["SELECT * FROM table"]
        (select * :table))
(expect ["SELECT id FROM table"]
        (select :id :table))
(expect ["SELECT id,name FROM table"]
        (select [:id :name] :table))
(expect ["SELECT id AS foo,name FROM table"]
        (select [{:id :foo} :name] :table))
(expect ["SELECT t.id FROM table t"]
        (select :t.id {:table :t}))

(expect "`a`" ((quoted \`) "a"))
(expect "`A`" ((quoted \`) "A"))
(expect "a" (as-is "a"))
(expect "A" (as-is "A"))
(expect "a" (lower-case "a"))
(expect "a" (lower-case "A"))

(expect ["SELECT * FROM `table`"]
        (entities (quoted \`)
                  (select * :table)))
(expect ["SELECT * FROM table"]
        (entities as-is
                  (select * :table)))
(expect ["SELECT `t`.`id` FROM `table`"]
        (entities (quoted \`)
                  (select :t.id :table)))

(expect "JOIN t ON a.id = t.id"
        (join :t {:a.id :t.id}))

(expect ["SELECT * FROM a JOIN b ON a.id = b.id"]
        (select * :a (join :b {:a.id :b.id})))
(expect ["SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON c.id = b.id"]
        (select * :a
                (join :b {:a.id :b.id})
                (join :c {:c.id :b.id})))

(expect ["id = ?" 42]
        (where {:id 42}))
(expect ["id IS NULL"]
        (where {:id nil}))

(expect ["SELECT * FROM a WHERE c = ? AND b = ?" 3 2]
        (select * :a (where {:b 2 :c 3})))
(expect ["SELECT * FROM a WHERE c IS NULL AND b = ?" 2]
        (select * :a (where {:b 2 :c nil})))

(expect ["SELECT a.id,b.name FROM aa a JOIN bb b ON a.id = b.id WHERE b.test = ?" 42]
        (select [:a.id :b.name] {:aa :a}
                (join {:bb :b} {:a.id :b.id})
                (where {:b.test 42})))
(expect ["SELECT `a`.`id`,`b`.`name` FROM `aa` `a` JOIN `bb` `b` ON `a`.`id` = `b`.`id` WHERE `b`.`test` = ?" 42]
        (entities (quoted \`)
                  (select [:a.id :b.name] {:aa :a}
                          (join {:bb :b} {:a.id :b.id})
                          (where {:b.test 42}))))

(expect ["UPDATE a SET b = ?" 2]
        (update :a {:b 2}))
(expect ["UPDATE a SET b = ? WHERE c = ?" 2 3]
        (update :a {:b 2}
                (where {:c 3})))
(expect ["UPDATE `a` SET `b` = ? WHERE `c` = ?" 2 3]
        (entities (quoted \`)
                  (update :a {:b 2}
                          (where {:c 3}))))

(expect ["DELETE FROM a WHERE b = ?" 2]
        (delete :a (where {:b 2})))
(expect ["DELETE FROM a WHERE c IS NULL AND b = ?" 2]
        (delete :a (where {:b 2 :c nil})))
(expect ["DELETE FROM `a` WHERE `b` = ?" 2]
        (entities (quoted \`)
                  (delete :a (where {:b 2}))))

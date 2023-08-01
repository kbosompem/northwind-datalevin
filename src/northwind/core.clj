(ns northwind.core
  (:require [pod.huahaiy.datalevin :as d]))

(def northwind-schema (read-string (slurp "schema.edn")))

(def conn (d/get-conn "northwind" northwind-schema))

(def db (d/db conn))

(d/transact! conn [{:category/categoryname "Seafood",
                     :global/description "Seaweed and fish",
                     :category/picture
                     "0x151C2F00020000000D000E0014002100FFFFFFFF4269746D617020496D616765005061696E742E5069637475726500010500000200000007000000504272757368000000000000000000A0290000424D98290000000000005600000028000000AC00000078000000010004000000000000000000880B0000880B0000080000"}])



(clojure.pprint/pprint
 (d/q '[:find  ?a (count ?e)
        :where
        [?e ?a _]]
      db))

(ns northwind.core
  (:refer-clojure :exclude [load])
  (:require [pod.huahaiy.datalevin :as d]))

(def northwind-schema (read-string (slurp "schema.edn")))

(def northwind-data (read-string (slurp "data.edn")))

(def conn (d/get-conn "northwind" northwind-schema))

(def db (d/db conn))

(d/transact! conn northwind-data)

(clojure.pprint/pprint
 (d/q '[:find  ?a (count ?e)
        :where
        [?e ?a _]]
      db))

; Get Employee and their Boss
(clojure.pprint/pprint
(d/q '[:find  (pull ?e [[:person/firstname :as :fname]
                        [:person/lastname :as :lname]
                        [:db/id :as :id]
                        {[:employee/reportsto :as :boss]
                         [[:person/firstname :as :fname]
                          [:person/lastname :as :lname]
                          [:db/id :as :id]]}])
       :where
       [?e :employee/title]]
     db))

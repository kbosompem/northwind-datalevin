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

(comment
  ; Get Employee and their Boss
  (clojure.pprint/pprint
   (d/q '[:find (pull ?e [[:employee/firstname :as :fname]
                          [:employee/lastname :as :lname]
                          [:db/id :as :id]
                          {[:territory/territory :as :territories] [[:territory/description]]}
                          {[:employee/reportsto :as :boss]
                           [[:employee/firstname :as :fname]
                            [:employee/lastname :as :lname]
                            [:db/id :as :id]]}])
          :where
          [?e :employee/title]]
        db))

  ; Employees
  (d/q '[:find   (pull ?e [*])
         :where
         [?e :employee/title]] db)

  ; Suppliers
  (clojure.pprint/print-table
   (map first (d/q '[:find   (pull ?e [*])
                     :where
                     [?e :supplier/companyname]] db)))
  #{}
  
  (def z {:db/id -20002,
          :employee/photo
          "0x151C2F00020000000D000E0014002100FFFFFFFF4269746D617020496D616765005061696E742E506963747572650001050000020000000700000050427275736800000000000000000020540000424D20540000000000007600000028000000C0000000DF0000000100040000000000A0530000CE0E0000D80E0000000000",
          :employee/address "908 W. Capital Way",
          :employee/hiredate #inst "1992-08-14T05:00:00.000-00:00",
          :employee/notes
          "Andrew received his BTS commercial in 1974 and a Ph.D. in international marketing from the University of Dallas in 1981.  He is fluent in French and Italian and reads German.  He joined the company as a sales representative, was promoted to sales manager i",
          :employee/region "WA",
          :employee/city "Tacoma",
          :employee/titleofcourtesy "Dr.",
          :employee/birthdate #inst "1952-02-19T06:00:00.000-00:00",
          :employee/lastname "Fuller",
          :employee/phoneextension "3457",
          :employee/photopath "http://accweb/emmployees/fuller.bmp",
          :employee/postalcode "98401",
          :employee/homephone "(206) 555-9482",
          :employee/firstname "Andrew",
          :employee/country "USA",
          :employee/title "Vice President, Sales"})
  
  (-> z  keys sort)
  
  )
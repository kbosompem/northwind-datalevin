(ns northwind.core
  (:require [pod.huahaiy.datalevin :as d]
            [clojure.pprint :as pp])
  (:import [java.util Calendar]
           [java.util Date]))

(def schema-files [:category :customer :employee
                   :region 
                   :territory 
                   :supplier 
                   :shipper  
                   :product 
                   :order
                   :orderdetail  
                   :demographic
                   ])

; -- Delete DB
(.delete (java.io.File. "n1/data.mdb"))
(.delete (java.io.File. "n1/lock.mdb"))

(def northwind-schema (apply merge (map #(read-string (slurp (str "src/northwind/schema/" (name %) ".edn"))) schema-files)))

(def conn (d/get-conn "n1" northwind-schema))

(def db (d/db conn))

(def northwind-data (read-string (slurp "data.edn")))

(d/transact! conn northwind-data)

; -- UTILITY FUNCTIONS 
(defn get-year [dt]
  (let [cal (Calendar/getInstance)
        _ (.setTime cal dt)]
    (.get cal Calendar/YEAR)))

(def pt pp/print-table)

; -- QUERIES 
; -- https://www.geeksengine.com/database/problem-solving/northwind-queries-part-1.php

(defn order-subtotals 
  "Order SubTotals
   
   (pt (sort-by :orderid (order-subtotals)))"
  []
  (d/q '[:find  ?o (sum ?s)
         :keys orderid subtotal
         :where
         [?e :orderdetail/order ?o]
         [?e :orderdetail/quantity ?q]
         [?e :orderdetail/unitprice ?up]
         [?e :orderdetail/discount ?d]
         [(- 1 ?d) ?x]
         [(* ?up ?q ?x) ?s]] db))

(defn sales-by-year
  "Sales by Year
   
   (pt (sort-by :orderid (sales-by-year)))"
  []
  (d/q '[:find ?sd ?o (sum ?s) ?yr
         :keys shippeddate orderid subtotal year
         :where
         [?e :orderdetail/order ?o]
         [?e :orderdetail/quantity ?q]
         [?e :orderdetail/unitprice ?up]
         [?e :orderdetail/discount ?d]
         [?o :order/shippeddate ?sd]
         [(get-year ?sd) ?yr]
         [(- 1 ?d) ?x]
         [(* ?up ?q ?x) ?s]] db))




(comment
(println :bad-attributes
         (remove second (d/q '[:find   ?e ?a ?v
                               :where
                               [?e ?a ?v]] db)))
 
  (clojure.pprint/pprint
    (sort-by first (d/q '[:find  ?o (sum ?s)
                          :where
                          [?e :orderdetail/order ?o]
                          [?e :orderdetail/quantity ?q]
                          [?e :orderdetail/unitprice ?up]
                          [?e :orderdetail/discount ?d]
                          [(- 1 ?d) ?x]
                          [(* ?up ?q ?x) ?s]] db)))
  

   (d/q '[:find  (pull ?e [[:orderdetail/order]])
                        :where
                        [?e :orderdetail/order ?o]
                        [?e :orderdetail/quantity ?q]
                        [?e :orderdetail/unitprice ?up]
                        [?e :orderdetail/discount ?d]
                        [(- 1 ?d) ?x]
                        [(* ?up ?q ?x) ?s]] db)

    (d/q '[:find  ?a (count ?e) :where [?e ?a _]] db)
    (d/q '[:find  ?a (count ?e) :where [?e ?a _]] db)
    (d/q '[:find  (count ?e) :where [?e]] db)
    (d/q '[:find (pull ?e [*])
           :where
           [?e :shipper/companyname]] db)

    (:product/productname northwind-schema)
    (d/transact! conn [{:order/shipvia "3"
                        :order/shippostalcode "51100"
                        :order/shipaddress "59 rue de l'Abbaye"
                        :order/shipname "Vins et alcools Chevalier"
                        :order/requireddate #inst "1996-08-01T05:00:00.000-00:00"
                        :order/freight 32.38
                        :order/employee  104
                        :db/id  -40248
                        :order/shippeddate #inst "1996-07-16T05:00:00.000-00:00"
                        :order/customer [:customer/customerid   "VINET"]
                        :order/shipregion "NULL"
                        :order/orderdate #inst "1996-07-04T05:00:00.000-00:00"
                        :order/shipcity "Reims"}])
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

    (d/q '[:find ?e ?a ?v
           :where
           [?e :order/shipname]
           [?e ?a ?v]] db)
    (clojure.pprint/print-table
     (map first (d/q '[:find   (pull ?e [*])
                       :where
                       [?e :order/shipname]] db)))

    (remove second (d/q '[:find   ?e ?a ?v
                          :where
                          [?e ?a ?v]] db))

    (d/q '[:find    ?a ?v
           :where
           [23 ?a ?v]] db)

  ; Employees
    (d/q '[:find   (pull ?e [*])
           :where
           [?e :employee/title]] db)

  ; Suppliers
    (clojure.pprint/print-table
     (map first (d/q '[:find   (pull ?e [*])
                       :where
                       [?e :supplier/companyname]] db)))

  ; Shippers
    (clojure.pprint/print-table
     (map first (d/q '[:find   (pull ?e [*])
                       :where
                       [?e :shipper/companyname]] db)))

   ; Products
    (clojure.pprint/print-table
     (map first (d/q '[:find   (pull ?e [[:product/productname :as product]])
                       :where
                       [?e :product/productname]] db)))


    (d/transact! conn [{:order/shipvia "3"
                        :order/shippostalcode "51100"
                        :order/shipaddress "59 rue de l'Abbaye"
                        :order/shipname "Vins et alcools Chevalier"
                        :order/shipcountry "France"
                        :order/requireddate #inst "1996-08-01T05:00:00.000-00:00"
                        :order/freight 32.38
                        :order/employee  [:employee/firstname "Adam"]
                      ;:db/id  -40248
                        :order/shippeddate #inst "1996-07-16T05:00:00.000-00:00"
                        :order/customer [:customer/customerid   "VINET"]
                        :order/shipregion "NULL"
                        :order/orderdate #inst "1996-07-04T05:00:00.000-00:00"
                        :order/shipcity "Reims"}])
    #{})
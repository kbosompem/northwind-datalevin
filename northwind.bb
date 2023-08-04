#!/usr/bin/env bb
(require '[babashka.pods :as pods])
(pods/load-pod 'huahaiy/datalevin "0.8.18")
(clojure.core/load-file "src/northwind/core.clj")
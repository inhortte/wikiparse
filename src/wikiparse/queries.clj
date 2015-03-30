(ns wikiparse.queries
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as es-index]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query :as q]))

(esr/connect "http://localhost:9200")
(def index "thurkpedia")

(def autism (esd/search index "page" :query (q/match :categories_tokenized "neurological disorders")))

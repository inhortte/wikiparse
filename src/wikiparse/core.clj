(ns wikiparse.core
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as es-index]
            [clojurewerkz.elastisch.rest.bulk :as es-bulk])
  (:import (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (java.util.concurrent.atomic AtomicLong))
  (:gen-class))

(def small-bz2 "./wikisample.xml.bz2")
(def thurk-bz2 "./thurkwiki.xml.bz2")

(defn bz2-reader
  "Returns a streaming Reader for the given compressed BZip2
  file. Use within (with-open)."
  [filename]
  (-> filename io/file io/input-stream BZip2CompressorInputStream. io/reader))

;; XML Mapping functions + helpers

(defn elem->map
  "Turns a list of elements into a map keyed by tag name. This doesn't
   work so well if tag names are repeated"
  [mappers]
  (fn [elems]
    (reduce (fn [m elem]
              (if-let [mapper ((:tag elem) mappers)]
                (reduce #(assoc %1 (first %2) ((second %2) elem)) m (partition 2 mapper))
                m))
            {} elems)))

;; modified from the original to expunge reference links, categories, etc, leaving the essential text.
(defn text-mapper
  [revision]
  (-> revision
      :content
      first
      (string/replace #"\{\{.+\}\}" "")
      (string/replace #"\[\[.+\]\]" "")
      (string/replace #"<ref.*>.+</ref>" "")
      (string/replace #"<.*ref>" "")
      (string/replace #"==.+==" "")))

(def content-mapper (comp first :content))

(defn categories-mapper
  [revision]
  (clojure.string/join ":::"
                       (map
                        (comp #(clojure.string/replace % "|" "")
                              clojure.string/trim second)
                        (re-seq #"\[\[Category:(.+)\]\]"
                                ((comp first :content) revision))))
  )

(def int-mapper #(Integer/parseInt (content-mapper %)))

(defn attr-mapper
  [attr]
  (fn [{attrs :attrs}]
    (get attrs attr)))

(def revision-mapper
  (comp
   (elem->map
    {:text [:categories categories-mapper :text text-mapper]
     :timestamp [:timestamp content-mapper]
     :format [:format (comp keyword content-mapper)]})
   :content))

;; note - :redirect should not exist since pages with redirect tags are filtered
;; out during filter-page-elems.
;; {:parsed-key [:indended-key extraction-function :intended-key extraction-function ....],
;; to be reduced with a (partition 2 ...)
(def page-mappers
  {:title [:title content-mapper]
   :ns [:ns int-mapper]
   :id [:id int-mapper]
   :redirect [:redirect (attr-mapper :title)]
   :revision [:revision revision-mapper]})

;; Parse logic

(defn match-tag
  "match an element by tag name"
  [tag-name]
  #(= tag-name (:tag %)))

;; Plus filter out redirects - Is it possible to do this in one operation?
(comment
  (defn filter-page-elems
    [wikimedia-elems]
    (filter (comp not (partial some (match-tag :redirect)) :content) (filter (match-tag :page) wikimedia-elems))))

(defn filter-page-elems
  [wikimedia-elems]
  (reduce (fn [appropriate-elems to-be-scrutinized]
            (if (and ((match-tag :page) to-be-scrutinized)
                     ((comp not (partial some (match-tag :redirect)) :content) to-be-scrutinized))
              (conj appropriate-elems to-be-scrutinized)
              appropriate-elems))
          [] wikimedia-elems))

(comment
  (defn nix-redirects
    [wikimedia-elems]
    (filter (comp not (partial some #(match-tag :redirect)) :content) wikimedia-elems)))

(defn isolate-categories
  [page]
  (let [text (:content ((match-tag :text) (:content ((match-tag :revison) (:content page)))))]
    )
  )

(defn xml->pages
  [parsed]
  (pmap (comp (elem->map page-mappers) :content)
        (filter-page-elems (:content parsed))))

;; Elasticsearch indexing

(defn bulk-index-pages
  [pages]
  ;; unnest command / doc tuples with apply concat
  (let [resp (es-bulk/bulk (apply concat pages) :consistency "one")]
    (when ((comp not = :ok) resp)
      (println resp))))

(defn es-page-formatter-for
  "returns an fn that formats the page as a bulk action tuple for a given index"
  [index-name]
  (fn
    [{title :title redirect :redirect {text :text categories :categories} :revision :as page}]
    ;; the target-title ensures that redirects are filed under the article they are redirects for
    (let [target-title (or redirect title)]
      [{:update {:_id (string/lower-case target-title) :_index index-name :_type :page}}
       {:script "if (is_redirect) {ctx._source.redirects += redirect};
               ctx._source.suggest.input += title;
               if (!is_redirect) { ctx._source.title = title; ctx._source.body = body};"
        :params {:redirect title, :title title
                 :target_title target-title, :body text
                 :is_redirect (boolean redirect)}
        :upsert {:title target-title
                 :redirects (if redirect [title] [])
                 :suggest {:input [title] :output target-title}
                 :body (when (not redirect) text)
                 :categories (when (not redirect) categories)}}])))

(defn es-format-pages
  [pages index-name]
  (map (es-page-formatter-for index-name) pages))

(defn phase-filter
  [phase]
  (cond (= :redirects phase) :redirect
        (= :full phase) (comp nil? :redirect)
        :else nil))

(defn filter-pages
  [pages phase]
  (filter (phase-filter phase) (filter #(= 0 (:ns %)) pages)))

(defn index-pages
  [pages callback]
  (pmap (fn [ppart]
         (bulk-index-pages ppart)
         (callback ppart))
       (partition-all 100 pages)))

(def categories-analyzer
  {:analysis {:analyzer {:categories-analyzer {:type "pattern", :pattern ":::"}}}})

(def page-mapping
  {
   :properties
   {
    :ns {:type :string :index :not_analyzed}
    :redirect {:type :string :index :not_analyzed}
    :title {
            :type :multi_field
            :fields
            {
             :title_snow {:type :string :analyzer :snowball}
             :title_simple {:type :string :analyzer :simple}
             :title_exact {:type :string :index :not_analyzed}}}
    :redirects {
                :type :multi_field
                :fields
                {
                 :redirects_snow {:type :string :analyzer :snowball}
                 :redirects_simple {:type :string :analyzer :simple}
                 :redirects_exact {:type :string :index :not_analyzed}}}
    :body {
           :type :multi_field
           :fields
           {
            :body_snow {:type :string :analyzer :snowball}
            :body_simple {:type :string :analyzer :simple}}}
    :categories {
                 :type :multi_field
                 :fields
                 {
                  :categories_tokenized {:type :string :analyzer :categories-analyzer}}}
    :suggest {
              :type :completion
              :index_analyzer :simple
              :search_analyzer :simple}}})

;; Bootstrap + Run

(defn ensure-index
  [name]
  (when (not (es-index/exists? name))
    (println (format "Deleting index %s" name))
    (es-index/delete name)
    (println (format "Creating index %s" name))
    (es-index/create name :mappings {:page page-mapping} :settings categories-analyzer)))

(defn index-dump
  [rdr callback phase index-name]
  ;; Get a reader for the bz2 file
  (-> rdr
      ;; Return a data.xml lazy parse-seq
      (xml/parse)
      ;; turn the seq of elements into a seq of maps
      (xml->pages)
      ;; Filter only ns 0 pages (only include 'normal' wikipedia articles)
      ;; Also, indicate whether we're processing redirects or full articles
      ;; in this pass. Redirects are indexed first, as re-tokenizing the full article
      ;; text with an update query is expensive. We want the article text to be the last
      ;; update
      (filter-pages phase)
      ;; re-map fields for elasticsearch
      (es-format-pages index-name)
      ;; send the fully formatted fields to elasticsearch
      (index-pages callback)))

(defn parse-cmdline
  [args]
  (let [[opts args banner]
        (cli/cli args
           "Usage: wikiparse [switches] path_to_bz2_wiki_dump"
           ["-h" "--help" "display this help and exit"]
           ["--es" "elasticsearch connection string" :default "http://localhost:9200"]
           ["--index" "elasticsearch index name" :default "en-wikipedia"])]
    (when (or (empty? args) (:help opts))
      (println banner)
      (System/exit 1))
    [opts (first args)]))

;; (doseq [phase [:redirects :full]] ... becomes simply [phase [:full]] because
;; redirects are eliminated early on.
(defn -main
  [& args]
  (let [[opts path] (parse-cmdline args)]
    (esr/connect! (:es opts)
                  (ensure-index (:index opts))
                  (let [counter (AtomicLong.)
                        callback (fn [pages] (println (format "@ %s pages" (.addAndGet counter (count pages)))))]
                    (doseq [phase [:full]]
                      (with-open [rdr (bz2-reader path)]
                        (println (str "Processing " phase))
                        (dorun (index-dump rdr callback phase (:index opts)))))
                    (println (format "Indexed %s pages" (.get counter))))
                  (System/exit 0))))

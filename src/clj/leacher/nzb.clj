(ns leacher.nzb
  (:require [clojure.data.zip.xml :as zip-xml]
            [clojure.java.io :as io]
            [clojure.xml :as xml]
            [clojure.zip :as zip]

            )
  (:import (clojure.lang XMLHandler)
           (java.io BufferedInputStream)
           (javax.xml.parsers SAXParser SAXParserFactory)))

(defn startparse-sax
  "Don't validate the DTDs, they are usually messed up."
  [^BufferedInputStream s ^XMLHandler ch]
  (let [factory (SAXParserFactory/newInstance)]
    (.setFeature factory "http://apache.org/xml/features/nonvalidating/load-external-dtd" false)
    (let [^SAXParser parser (.newSAXParser factory)]
      (.parse parser s ch))))

(defn attr
  [node attr]
  (zip-xml/xml1-> node (zip-xml/attr attr)))

(defn segment->map
  [seg]
  {:bytes      (Long/valueOf ^String (attr seg :bytes))
   :number     (Integer/valueOf ^String (attr seg :number))
   :message-id (str \< (zip-xml/xml1-> seg zip-xml/text) \>)})

(def filename-re #".*?\"(.*?)\".*")

(defn filename-from
  [subject]
  (if-let [[_ result] (re-find filename-re subject)]
    result
    (str (java.util.UUID/randomUUID))))

(def yenc-re #"(?i)yenc")

(defn encoding-from
  [subject]
  (if (re-find yenc-re subject) :yenc :unknown))

(defn ->file
  [file]
  (let [poster      (attr file :poster)
        date        (Long/valueOf ^String (attr file :date))
        subject     (attr file :subject)
        filename    (filename-from subject)
        encoding    (encoding-from subject)
        groups      (vec (zip-xml/xml-> file :groups :group zip-xml/text))
        segments    (->> (zip-xml/xml-> file :segments :segment)
                         (map segment->map)
                         (map (juxt :message-id identity))
                         (into {}))
        total-bytes (reduce + (map :bytes (vals segments)))]
    {:poster         poster
     :date           date
     :subject        subject
     :encoding       encoding
     :filename       filename
     :groups         groups
     :total-bytes    total-bytes
     :total-segments (count segments)
     :segments       segments}))

(defn parse
  [input]
  (-> (io/input-stream input)
      (xml/parse startparse-sax)
      zip/xml-zip
      (zip-xml/xml-> :file)
      (->> (mapv ->file)
        (mapv (juxt :filename identity))
        (into {}))))

(comment
  (first (parse "/home/gareth/.leacher/slayer.nzb"))

  )

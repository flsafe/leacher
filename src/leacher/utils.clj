(ns leacher.utils
  (:require [clojure.tools.logging :as log]))

(defn parse-long
  [^String s]
  (Long/valueOf s))

(defn parse-longs
  [& s]
  (mapv parse-long s))

(defmacro logt
  "Time the evaluation of the body and log it along with msg"
  [msg & body]
  `(let [start#  (System/currentTimeMillis)
         result# (do ~@body)]
     (log/info ~msg "took" (format "%.2fs" (* (- (System/currentTimeMillis) start#) 1e-3)))
     result#))

(ns leacher.utils)

(defn parse-long
  [^String s]
  (Long/valueOf s))

(defn parse-longs
  [& s]
  (mapv parse-long s))

(ns leacher.workers
  (:require [clojure.core.async :as async :refer [thread <!! >!! alt!! chan]]
            [clojure.tools.logging :as log]))

(defn worker
  [prefix ch body-fn & [n]]
  (log/debug prefix "starting")
  (thread
    (loop []
      (if-let [v (<!! ch)]
        (do
          (log/debug prefix "got" v)
          (try
            (if n
              (body-fn n v)
              (body-fn v))
            (catch Exception e
              (log/error e prefix "failed")))
          (recur))
        (log/info prefix "exiting")))))

(defn workers
  [n prefix ch body-fn]
  (dotimes [i n]
    (worker (format "%s[%d]" prefix i) ch body-fn i)))

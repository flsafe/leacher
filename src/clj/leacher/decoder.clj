(ns leacher.decoder
  (:require [clojure.core.async :as async
             :refer [<!! >!! chan put! thread alt!!]]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.decoders.yenc :as yenc])
  (:import [java.io RandomAccessFile]))

;; component

(defn decode-segment
  [n segment]
  (let [message-id (:message-id segment)]
    (if-let [segment-file (fs/file (:downloaded-path segment))]
      (yenc/decode segment-file)
      (log/infof "worker[%d]: no downloaded-path for %s, skipping" n message-id))))

(defn start-worker
  [{:keys [decodes shutdown]} n]
  (thread
    (loop []
      (alt!!
        shutdown
        ([_]
           (log/infof "worker[%d]: shutting down" n))

        decodes
        ([{:keys [segment reply file events] :as work}]
           (when work
             (>!! reply
               (try
                 (let [decoded (decode-segment n segment)]
                   (put! events {:type :segment-decode-complete
                                 :file file})
                   (assoc work :decoded decoded))
                 (catch Exception e
                   (log/errorf e "worker[%d]: failed decoding" n (:message-id segment))
                   (put! events {:type    :segment-decode-failed
                                 :file    file
                                 :message (.getMessage e)})
                   (assoc work :error e))))
             (recur)))

        :priority true))))

(defn start-workers
  [channels]
  (doall
    (mapv (partial start-worker channels)
      (range 10))))

(defrecord Decoder [workers channels]
  component/Lifecycle
  (start [this]
    (if-not workers
      (let [workers (start-workers channels)]
        (log/info "starting")
        (assoc this :workers workers))
      this))

  (stop [this]
    (if channels
      (do (log/info "stopping")
          (assoc this :workers nil))
      this)))

(defn new-decoder
  []
  (map->Decoder {}))

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
  (log/info decodes)
  (log/infof "worker[%d]: starting" n)
  (thread
    (loop []
      (log/infof "worker[%d]: waiting for" n)
      (alt!!
        decodes
        ([{:keys [segment reply] :as work}]
           (when work
             (log/infof "worker[%d]: got segment %s" n (:message-id segment))
             (>!! reply
               (try
                 (assoc work :decoded (decode-segment n segment))
                 (catch Exception e
                   (log/errorf e "worker[%d]: failed downloading" n (:message-id segment))
                   (assoc work :error e))))
             (recur)))
        shutdown
        ([_]
           (log/infof "worker[%d]: shutting down" n))))))

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

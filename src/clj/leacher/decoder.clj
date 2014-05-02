(ns leacher.decoder
  (:require [clojure.core.async :as async
             :refer [<!! >!! chan put! thread]]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [leacher.decoders.yenc :as yenc]
            [leacher.state :as state]
            [leacher.workers :refer [worker workers]])
  (:import [java.io RandomAccessFile]))

;; component

(defn write-to
  [{:keys [segments filename]} file]
  (let [output (RandomAccessFile. ^java.io.File file "rw")]
    (doseq [segment (vals segments)
            :let [decoded (:decoded segment)]
            :when decoded]
      (let [begin (-> decoded :keywords :part :begin dec)
            end   (-> decoded :keywords :part :end dec)]
        (.seek output begin)
        (.write output ^bytes (:bytes decoded))))))

(defn decode-segment
  [n {:keys [segment reply] :as val}]
  (try
    (log/debugf "worker[%d]: got %s" n (:message-id segment))
    (if-let [segment-file (:downloaded segment)]
      (let [decoded (yenc/decode segment-file)]
        (>!! reply (-> val
                     (dissoc :reply)
                     (assoc-in [:segment :decoded] decoded))))
      (do
        (log/warnf "worker[%d]: no downloaded, skipping" n)
        (>!! reply (dissoc val :reply))))
    (catch Exception e
      (log/errorf e "worker[%d]: failed decoding" n)
      ;; TODO: handle error in some way
      (>!! reply (dissoc val :reply)))))

(defn start-workers
  [{:keys [decoders]} {:keys [work]} app-state]
  (workers decoders "yenc-worker" work decode-segment))

(defn decode-file
  [file work]
  (let [segments (:segments file)
        replies  (chan (count segments))]
    ;; put all segments onto work queue for decoding concurrently
    ;; across workers
    (doseq [segment (vals segments)]
      (put! work
            {:reply   replies
             :segment segment}))

    ;; combine the results back onto the original file->segments
    (async/reduce (fn [res {:keys [segment]}]
                    (assoc-in res [:segments (:message-id segment)] segment))
                  file (async/take (count segments) replies))))

(defn process-file
  [cfg app-state {:keys [out work]} {:keys [filename] :as file}]
  (try
    (log/debug "got file" filename)
    (state/update-file! app-state filename assoc
      :status :decoding
      :decoding-started-at (System/currentTimeMillis))

    (let [file     (<!! (decode-file file work))
          combined (io/file (-> cfg :dirs :temp) filename)]
      (io/make-parents combined)
      (write-to file combined)

      (state/update-file! app-state filename assoc
        :status :waiting
        :decoding-finished-at (System/currentTimeMillis))

      (>!! out
        (assoc file :combined combined)))
    (catch Exception e
      (log/error e "failed decoding" filename)
      (>!! out file))))

(defn start-listening
  [cfg {:keys [in] :as channels} app-state]
  (worker "yenc-listener" in
    (fn [file]
      (process-file cfg app-state channels file))))

(defrecord Decoder [cfg channels app-state]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in   (chan 10)
                      :out  (chan 10)
                      :work (chan)}]
        (log/info "starting")
        (start-workers cfg channels app-state)
        (start-listening cfg channels app-state)
        (assoc this :channels channels))
      this))

  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels channels))
      this)))

(defn new-decoder
  [cfg]
  (map->Decoder {:cfg cfg}))

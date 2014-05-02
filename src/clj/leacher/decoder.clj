(ns leacher.decoder
  (:require [clojure.core.async :as async
             :refer [<!! >!! chan put! thread]]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
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
  [app-state n {:keys [filename message-id reply]}]
  (let [segment (state/get-segment app-state filename message-id)]
    (if (:cancelled segment)
      (do
        (log/infof "worker[%d]: %s cancelled, skipping" n message-id)
        (>!! reply :cancelled))
      (try
        (log/debugf "worker[%d]: got %s" n message-id)
        (if-let [segment-file (fs/file (:downloaded segment))]
          (let [decoded (yenc/decode segment-file)]
            (state/update-segment! app-state filename message-id assoc
              :status :decoded
              :decoded decoded)
            (>!! reply :completed))
          (do
            (log/warnf "worker[%d]: no downloaded, skipping" n)
            (>!! reply :failed)))
        (catch Exception e
          (log/errorf e "worker[%d]: failed decoding" n)
          (>!! reply :failed))))))

(defn start-workers
  [{:keys [decoders]} {:keys [work]} app-state]
  (workers decoders "yenc-worker" work (partial decode-segment app-state)))

(defn decode-file
  [file work]
  (let [message-ids (mapv :message-id (-> file :segments vals))
        number      (count message-ids)
        replies     (chan number)]

    (doseq [message-id message-ids]
      (put! work
        {:reply      replies
         :filename   (:filename file)
         :message-id message-id}))

    (let [results (<!! (async/reduce conj [] (async/take number replies)))]
      (cond
        (every? #(= :completed %) results) :decoded
        (some #{:cancelled} results)       :cancelled
        (some #{:failed} results)          :failed))))

(defn process-file
  [cfg app-state {:keys [out work]} {:keys [filename] :as file}]
  (try
    (log/debug "got file" filename)
    (state/update-file! app-state filename assoc
      :status :decoding
      :decoding-started-at (System/currentTimeMillis))

    (let [result   (decode-file file work)
          combined (io/file (-> cfg :dirs :temp) filename)]
      (io/make-parents combined)
      (let [file (state/get-file app-state filename)]
        (write-to file combined))

      (state/update-file! app-state filename assoc
        :status result
        :combined (fs/absolute-path combined)
        :decoding-finished-at (System/currentTimeMillis))

      (when (= :decoded result)
        (>!! out filename)))
    (catch Exception e
      (log/error e "failed decoding" filename))))

(defn start-listening
  [cfg {:keys [in] :as channels} app-state]
  (worker "yenc-listener" in
    (fn [filename]
      (let [file (state/get-file app-state filename)]
        (when-not (:cancelled file)
          (process-file cfg app-state channels file))))))

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

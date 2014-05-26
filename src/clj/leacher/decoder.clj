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
  [file ^java.io.File to-file]
  (let [output   (RandomAccessFile. to-file "rw")
        segments (:segments @file)]
    (doseq [segment (vals segments)
            :let [decoded (:decoded segment)]
            :when decoded]
      (let [begin (-> decoded :keywords :part :begin dec)
            end   (-> decoded :keywords :part :end dec)]
        (.seek output begin)
        (.write output ^bytes (:bytes decoded))))))

(defn decode-segment
  [n {:keys [file segment reply]}]
  (let [message-id (:message-id @segment)]
    (if (:cancelled @segment)
      (do
        (log/infof "worker[%d]: %s cancelled, skipping" n message-id)
        (>!! reply :cancelled))
      (try
        (log/debugf "worker[%d]: got %s" n message-id)
        (if-let [segment-file (fs/file (:downloaded @segment))]
          (let [decoded (yenc/decode segment-file)]
            (state/assoc! segment :status :decoded :decoded decoded)
            (>!! reply :completed))
          (do
            (log/warnf "worker[%d]: no downloaded, skipping" n)
            (>!! reply :failed)))
        (catch Exception e
          (log/errorf e "worker[%d]: failed decoding" n)
          (>!! reply :failed))))))

(defn start-workers
  [{:keys [decoders]} {:keys [work]}]
  (workers decoders "yenc-worker" work decode-segment))

(defn decode-file
  [file work]
  (let [message-ids (mapv :message-id (-> @file :segments vals))
        number      (count message-ids)
        replies     (chan number)]

    (doseq [message-id message-ids
            :let [segment (state/new-scope file :segments message-id)]]
      (put! work
        {:reply   replies
         :file    file
         :segment segment}))

    (let [results (<!! (async/reduce conj [] (async/take number replies)))]
      (cond
        (every? #(= :completed %) results) :decoded
        (some #{:cancelled} results)       :cancelled
        (some #{:failed} results)          :failed))))

(defn process-file
  [cfg app-state {:keys [out work]} file]
  (let [filename (:filename @file)]
   (try
     (log/info "got file" filename)
     (state/assoc! file
       :status :decoding
       :decoding-started-at (System/currentTimeMillis))

     (let [result   (decode-file file work)
           combined (io/file (-> cfg :dirs :temp) filename)]
       (io/make-parents combined)
       (write-to file combined)

       ;; throw away the decoded bytes to ensure they are GC'd
       (state/update-in! file [:segments]
         (fn [m]
           (reduce-kv #(assoc %1 %2 (dissoc %3 :decoded)) {} m)))

       (state/assoc! file
         :status result
         :combined (fs/absolute-path combined)
         :decoding-finished-at (System/currentTimeMillis))

       (when (not= :cancelled result)
         (>!! out file)))
     (catch Exception e
       (log/error e "failed decoding" filename)))))

(defn start-listening
  [cfg {:keys [in] :as channels} app-state]
  (worker "yenc-listener" in
    (fn [file]
      (when-not (:cancelled @file)
        (process-file cfg app-state channels file)))))

(defrecord Decoder [cfg channels app-state]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in   (chan 10)
                      :out  (chan 10)
                      :work (chan)}]
        (log/info "starting")
        (start-workers cfg channels)
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

(ns leacher.pipeline
  (:require [leacher.nzb :as nzb]
            [leacher.nntp :as nntp]
            [leacher.decoders.yenc :as yenc]
            [leacher.config :as config]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go go-loop >! <! onto-chan chan <!! >!! alt! put! close!]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import [java.io RandomAccessFile]))

(defn ^java.io.File file->temp-file
  [file]
  (fs/file config/tmp-dir (:filename file)))

(defn ^java.io.File file->complete-file
  [file]
  (fs/file config/complete-dir (:filename file)))

(defn cleanup
  [{:keys [filename] :as file} {:keys [events]}]
  (put! events {:type     :file-status
                :status   :cleanup-starting
                :filename filename})
  (doseq [segment (-> file :segments vals)
          :let [f (fs/file (:downloaded-path segment))]
          :when (and f (fs/exists? f))]
    (fs/delete f))
  (let [combined (file->temp-file file)
        complete (file->complete-file file)]
    (log/debug "moving" (str combined) "to" (str complete))
    (fs/rename combined complete))
  (put! events {:type     :file-status
                :status   :completed
                :filename filename}))

(defn write-to-file
  [file decoded]
  (let [to-file (file->temp-file file)
        begin   (-> decoded :keywords :part :begin dec)
        end     (-> decoded :keywords :part :end dec)]
    (with-open [output (RandomAccessFile. to-file "rw")]
      (.seek output begin)
      (.write output ^bytes (:bytes decoded)))))

(defn decode
  [{:keys [filename] :as file} {:keys [events decodes]}]
  (put! events {:type     :file-status
                :status   :decoding
                :filename filename})
  (let [reply (chan)]
    (log/info "decoding segments for" filename)
    (go
      (doseq [[message-id segment] (:segments file)]
        (>! decodes {:reply   reply
                     :events  events
                     :file    file
                     :segment segment})))
    (log/infof "waiting for decoding of %d segments for %s"
      (:total-segments file) filename)
    (let [replies (async/take (:total-segments file) reply)]
      (loop []
        (when-let [{:keys [file segment decoded]} (<!! replies)]
          (if decoded
            (write-to-file file decoded)
            (log/warn "no decoded section to write for" (:message-id segment)))
          (recur)))))
  (put! events {:type     :file-status
                :status   :decode-complete
                :filename filename}))

(defn download
  [{:keys [filename] :as file} {:keys [events downloads]}]
  (put! events {:type     :file-status
                :status   :downloading
                :filename filename})
  (let [reply (chan)]
    (log/info "queueing segments for" filename)
    (go
      (doseq [[message-id segment] (:segments file)]
        (>! downloads {:reply   reply
                       :events  events
                       :file    file
                       :segment segment})))
    (log/infof "waiting for downloads of %d segments for %s"
      (:total-segments file) filename)
    (let [replies (async/take (:total-segments file) reply)
          result (loop [result file]
                   (if-let [{:keys [error downloaded-path segment]} (<!! replies)]
                     (recur (cond-> result
                              error
                              (assoc-in [:segments (:message-id segment)
                                         :error] error)

                              downloaded-path
                              (assoc-in [:segments (:message-id segment)
                                         :downloaded-path]
                                downloaded-path)))
                     result))]
      (put! events {:type     :file-status
                    :status   :download-complete
                    :filename filename})
      result)))

(defn process-file
  [file {:keys [events] :as channels}]
  (let [file (download file channels)]
    (decode file channels)
    (cleanup file channels)))

(defn start-worker
  [{:keys [watcher shutdown] :as channels} work-ch n]
  (go-loop []
    (alt!
      shutdown
      ([_] (log/infof "worker[%d]: shutting down" n))

      work-ch
      ([file]
         (when file
           (try
             (process-file file channels)
             (catch Exception e
               (log/error e "failed processing" (:filename file))))
           (recur)))

      :priority true)))

(defn start-listener
  "Listens for messages from the watcher, which are new nzb files in
  the queue directory."
  [{:keys [watcher shutdown events] :as channels} work-ch]
  (go-loop []
    (alt!
      shutdown
      ([_]
         (log/info "shutting down"))

      watcher
      ([f]
         (when f
           (try
             (log/info "got new file" (str f))
             (let [files (for [[_ file] (-> (nzb/parse f) :files sort)
                               :when (not (fs/exists? (file->complete-file file)))]
                           file)]
               (doseq [file files]
                 (put! events {:type :download-pending
                               :file file}))
               (doseq [file files]
                 (>! work-ch file)))
             (catch Exception e
               (log/error e "failed processing" (str f))))
           (recur)))

      :priority true)))

;;

(defrecord Pipeline [channels workers listener work-ch]
  component/Lifecycle
  (start [this]
    (if-not workers
      (let [work-ch  (chan)
            listener (start-listener channels work-ch)
            workers  (doall (mapv (partial start-worker channels work-ch) (range 5)))]
        (log/info "starting")
        (assoc this
          :work-ch work-ch
          :workers workers
          :listener listener))
      this))
  (stop [this]
    (if workers
      (do (log/info "stopping")
          (async/close! work-ch)
          (assoc this
            :work-ch nil
            :workers nil
            :listener nil))
      this)))

(defn new-pipeline
  []
  (map->Pipeline {}))


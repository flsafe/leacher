(ns leacher.pipeline
  (:require [leacher.nzb :as nzb]
            [leacher.nntp :as nntp]
            [leacher.decoders.yenc :as yenc]
            [leacher.config :as config]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go go-loop >! <! onto-chan chan <!! >!! alt! put!]]
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
  (put! events {:type :cleanup-starting :file file})
  (doseq [segment (-> file :segments vals)
          :let [f (fs/file (:downloaded-path segment))]
          :when (and f (fs/exists? f))]
    (fs/delete f))
  (let [combined (file->temp-file file)
        complete (file->complete-file file)]
    (log/debug "moving" (str combined) "to" (str complete))
    (fs/rename combined complete))
  (put! events {:type :cleanup-complete :file file}))

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
  (put! events {:type :decode-starting :file file})
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
  (put! events {:type :decode-complete :file file}))

(defn download
  [{:keys [filename] :as file} {:keys [events downloads]}]
  (put! events {:type :download-starting :file file})
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
          result  (loop [result file]
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
      (put! events {:type :download-complete :file file})
      result)))

(defn process-file
  [f {:keys [events] :as channels}]
  (doseq [[_ file] (-> (nzb/parse f) :files sort)
          :when (not (fs/exists? (file->complete-file file)))]
    (let [file (download file channels)]
      (decode file channels)
      (cleanup file channels))))

;;

(defn start-worker
  [{:keys [watcher shutdown] :as channels} n]
  (go-loop []
    (alt!
      shutdown
      ([_]
         (log/infof "worker[%d]: shutting down" n))

      watcher
      ([f]
         (when f
           (log/infof "worker[%d]: got new file %s" n (str f))
           (process-file f channels)
           (recur)))

      :priority true)))

(defrecord Pipeline [channels workers]
  component/Lifecycle
  (start [this]
    (if-not workers
      (let [workers (doall
                      (mapv (partial start-worker channels)
                        (range 5)))]
        (log/info "starting")
        (assoc this :workers workers))
      this))
  (stop [this]
    (if workers
      (do (log/info "stopping")
          (assoc this :workers nil))
      this)))

(defn new-pipeline
  []
  (map->Pipeline {}))

(ns leacher.pipeline
  (:require [leacher.nzb :as nzb]
            [leacher.nntp :as nntp]
            [leacher.decoders.yenc :as yenc]
            [leacher.config :as config]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go go-loop >! <! onto-chan chan <!! >!! alt!]]
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
  [{:keys [nzb nzb-file]}]
  (log/debug "cleaning up temp files for" (str nzb-file))
  (doseq [[filename file]    (:files nzb)
          segment (-> file :segments vals)
          :let [f (fs/file (:downloaded-path segment))]
          :when (and f (fs/exists? f))]
    (fs/delete f))
  (doseq [[filename file] (:files nzb)
          :let [combined (file->temp-file file)
                complete (file->complete-file file)]]
    (log/debug "moving" (str combined) "to" (str complete))
    (fs/rename combined complete)))

(defn write-to-file
  [file decoded]
  (let [to-file (file->temp-file file)
        begin   (-> decoded :keywords :part :begin dec)
        end     (-> decoded :keywords :part :end dec)]
    (with-open [output (RandomAccessFile. to-file "rw")]
      (.seek output begin)
      (.write output ^bytes (:bytes decoded)))))

(defn decode
  [handler {:keys [decodes]}]
  (fn [{:keys [nzb nzb-file] :as m}]
    (let [reply (chan)]
      (log/info "decoding segments for" (str nzb-file))
      (go
        (doseq [[filename file] (sort (:files nzb))
                [message-id segment] (:segments file)]
          (>! decodes {:reply   reply
                       :nzb     nzb
                       :file    file
                       :segment segment})))
      (log/infof "waiting for decoding of %d segments for %s"
        (:total-segments nzb) (str nzb-file))
      (let [replies (async/take (:total-segments nzb) reply)]
        (loop []
          (when-let [{:keys [file segment decoded]} (<!! replies)]
            (if decoded
              (write-to-file file decoded)
              (log/warn "no decoded section to write for" (:message-id segment)))
            (recur)))))
    (log/info "finished decoding" (str nzb-file))
    (handler m)))

(defn download
  [handler {:keys [downloads]}]
  (fn [{:keys [nzb nzb-file] :as m}]
    (let [reply (chan)]
      (log/info "queueing segments for" (str nzb-file))
      (go
        (doseq [[filename file] (sort (:files nzb))
                [message-id segment] (:segments file)]
          (>! downloads {:reply   reply
                          :nzb     nzb
                          :file    file
                          :segment segment})))
      (log/infof "waiting for downloads of %d segments for %s"
        (:total-segments nzb) (str nzb-file))
      (let [replies (async/take (:total-segments nzb) reply)
            nzb     (loop [nzb nzb]
                      (if-let [{:keys [error downloaded-path segment file]} (<!! replies)]
                        (recur (cond-> nzb
                                 error
                                 (assoc-in [:files (:filename file)
                                            :segments (:message-id segment)
                                            :error] error)
                                 downloaded-path
                                 (assoc-in [:files (:filename file)
                                            :segments (:message-id segment)
                                            :downloaded-path]
                                   downloaded-path)))
                        nzb))]
        (log/info "finished downloading" (str nzb-file))
        (handler (assoc m :nzb nzb))))))

(defn parse
  [handler]
  (fn [{:keys [nzb-file] :as m}]
    (log/info "parsing" (str nzb-file))
    (let [nzb (nzb/parse nzb-file)]
      (handler (assoc m :nzb nzb))
      (fs/delete nzb-file))))

(defn log-errors
  [handler]
  (fn [m]
    (try (handler m)
         (catch Exception e
           (log/error e "unexpected error in pipeline")))))

(defn build-pipeline
  [channels]
  (-> cleanup
    (decode channels)
    (download channels)
    parse
    log-errors))

;;

(defn start-worker
  [pipeline-fn {:keys [watcher shutdown]}]
  (go-loop []
    (alt!
      watcher
      ([f]
         (log/info "got new file" (str f))
         (pipeline-fn {:nzb-file f})
         (recur))

      shutdown
      ([_]
         (log/info "shutting down")))))

(defrecord Pipeline [channels worker]
  component/Lifecycle
  (start [this]
    (if-not worker
      (let [pipeline-fn (build-pipeline channels)]
        (log/info "starting")
        (assoc this :worker (start-worker pipeline-fn channels)))
      this))
  (stop [this]
    (if worker
      (do (log/info "stopping")
          (assoc this :worker nil))
      this)))

(defn new-pipeline
  []
  (map->Pipeline {}))

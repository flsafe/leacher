(ns leacher.conductor
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [chan >!! <!! thread alt!!]]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [leacher.utils :refer [logt]]
            [leacher.state :as state]
            [leacher.watcher :as watcher]
            [leacher.nntp :as nntp]
            [leacher.nzb :as nzb]
            [leacher.decoders.yenc :as yenc])
  (:import [java.io RandomAccessFile]))

(defn worker
  [n4me body-fn & {:keys [in out ctl]
              :or   {in  (chan)
                     out (chan)
                     ctl (chan)}}]
  (let [channels {:in in :out out :ctl ctl}]
    (thread
      (loop []
        (log/infof "worker[%s]: waiting for work" n4me)
        (alt!!
          in
          ([val]
             (if val
               (do
                 (try
                   (body-fn channels val)
                   (catch Exception e
                     (log/errorf e "worker[%s]: error" n4me)))
                 (recur))
               (log/infof "worker[%s]: got nil value, exiting loop" n4me)))

          ctl
          ([_]
             (log/infof "worker[%s]: shutdown" n4me)))))
    channels))

(defn start-cleaner
  [cfg app-state]
  (worker "cleaner"
          (fn [channels {:keys [filename] :as file}]
            (log/info "cleaner got file:" filename)
            (doseq [segment (-> file :segments vals)
                    :let [f (:downloaded-file segment)]]
              (when f
                (log/info "cleaner deleting" (str f))
                (fs/delete f)))
            (let [combined (:combined-file file)
                  complete (fs/file (-> cfg :dirs :complete)
                                    (fs/base-name combined))]
              (log/info "cleaner moving" (str combined) "to" (str complete))
              (fs/rename combined complete))
            (state/set-state! app-state update-in [:downloads filename :status] :completed))))

(defn start-combiner
  [cfg app-state]
  (worker "combiner"
          (fn [channels file]
            (log/info "combiner got file:" (:filename file))
            (let [combined-file (io/file (-> cfg :dirs :temp) (:filename file))]
              (io/make-parents combined-file)
              (yenc/write-to file combined-file)
              (log/info "combiner putting on out chan")
              (>!! (:out channels)
                   (assoc file :combined-file combined-file))))))

(defn start-decoder
  [cfg app-state yenc-decoder]
  (worker "decoder"
          (fn [channels file]
            (log/info "decoder got file:" (:filename file))
            (let [result-ch (yenc/decode-file yenc-decoder file)]
              (log/info "decoder putting on out chan")
              (>!! (:out channels)
                   (<!! result-ch))))))

;; TODO can have multiple downloaders so can have several concurrent segments from different files downloading?
(defn start-downloader
  [cfg app-state nntp]
  (worker "downloader"
          (fn [channels {:keys [filename] :as file}]
            (log/info "downloader got file:" filename)
            (state/set-state! app-state assoc-in [:downloads filename :status] :starting)
            (let [result-ch (nntp/download nntp file)]
              (state/set-state! app-state assoc-in [:downloads filename :status] :downloading)
              (log/info "downloader putting on out chan")
              (>!! (:out channels)
                   (<!! result-ch))))))

(defn start-listener
  [cfg app-state]
  (worker "listener"
          (fn [channels path]
            (log/info "listener got path:" path)
            (let [files (reduce-kv #(assoc %1 %2 (assoc %3 :status :waiting))
                          (nzb/parse (io/file path)))]
              (state/set-state! app-state update-in [:downloads] merge files))
            (doseq [[filename file] (nzb/parse (io/file path))]
              (log/infof "listener %s putting on out chan" filename)
              (>!! (:out channels) file)))))

;; component

(defrecord Conductor [cfg app-state watcher nntp yenc-decoder ctls]
  component/Lifecycle
  (start [this]
    (if-not ctls
      (do
        (log/info "starting")
        (let [workers [{:out (:events watcher)}
                       (start-listener cfg app-state)
                       (start-downloader cfg app-state nntp)
                       (start-decoder cfg app-state yenc-decoder)
                       (start-combiner cfg app-state)
                       (start-cleaner cfg app-state)]
              ctls    (mapv :ctl (rest workers))]
          (doseq [[w1 w2] (partition 2 1 workers)
                  :let [from (:out w1)
                        to   (:in w2)]]
            (async/pipe from to))
          (assoc this :ctls ctls)))
      this))

  (stop [this]
    (if ctls
      (do
        (log/info "stopping")
        (doseq [ctl ctls]
          (async/close! ctl))
        (assoc this :ctls nil)
      this))))

(defn new-conductor
  [cfg]
  (map->Conductor {:cfg cfg}))

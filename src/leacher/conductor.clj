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
  [body-fn & {:keys [in out ctl]
              :or   {in  (chan 100)
                     out (chan 100)
                     ctl (chan)}}]
  (let [channels {:in in :out out :ctl ctl}]
    (thread
      (loop []
        (alt!!
          in
          ([val]
             (when val
               (try
                 (body-fn channels val)
                 (catch Exception e
                   (log/error e "worker: error")))
               (recur)))

          ctl
          ([_]
             (log/infof "worker: shutdown")))))
    channels))

(defn parallel-worker
  [workers body-fn]
  (let [work (chan 100)]
    (dotimes [n workers]
      (thread
        (loop []
          (alt!!
            work
            ([{:keys [val replies]}]
               (>!! replies
                    (body-fn n val))
               (recur))))))
    (worker (fn [channels vals]
              (let [replies (chan (count vals))]
                (thread
                  (doseq [val vals]
                    (>!! work {:replies replies :val val})))
                (>!! (:out channels)
                     (<!! (async/into [] (async/take (count vals) replies)))))))))

(defn start-cleaner
  [cfg app-state]
  (worker (fn [channels vals]
            (doseq [val vals]
              (let [seg-file (:segment-file val)]
                (log/info "cleaning up" (str seg-file))
                (fs/delete seg-file)))
            (let [decoded  (:decoded-file (first vals))
                  complete (fs/file (-> cfg :dirs :complete)
                                    (fs/base-name decoded))]
              (fs/rename decoded complete)))))

(defn start-segment-combiner
  [cfg app-state]
  (worker (fn [channels vals]
            (>!! (:out channels)
                 (mapv (fn [val]
                         (let [file (io/file (-> cfg :dirs :temp) (-> val :file :filename))]
                           (io/make-parents file)
                           (yenc/decode-to (:decoded-segment val) file)
                           (assoc val :decoded-file file)))
                       vals)))))

(defn start-decoder
  [cfg app-state]
  (parallel-worker 10 (fn [n val]
                        (logt (format "decoder: decoding file:" (str (:segment-file val)))
                              (let [seg-file (:segment-file val)
                                    decoded  (yenc/decode-segment seg-file)]
                                (assoc val :decoded-segment decoded))))))

(defn start-writer
  [cfg app-state]
  (parallel-worker 10
                   (fn [n val]
                     (let [dir  (-> cfg :dirs :temp)
                           file (io/file dir (-> val :segment :message-id))]
                       (logt (format "writer[%d]: writing segment to %s" n (str file))
                             (io/copy (-> val :resp :bytes) file)
                             (assoc val :segment-file file))))))

(defn start-downloader
  [cfg app-state nntp]
  (worker (fn [channels val]
            (log/info "starting download of file" (:filename val))
            (let [replies  (nntp/download nntp val)
                  segments (count (:segments val))]
              (>!! (:out channels)
                   (<!! (async/into [] (async/take segments replies))))))))

(defn start-listener
  [cfg app-state]
  (worker (fn [channels path]
            (log/info "received nzb file:" path)
            (doseq [file (nzb/parse (io/file path))]
              (>!! (:out channels)
                   (assoc file :nzb-file file))))))

;; component

(defrecord Conductor [cfg app-state watcher nntp ctls]
  component/Lifecycle
  (start [this]
    (if-not ctls
      (do
        (log/info "starting")
        (let [listener   (start-listener cfg app-state)
              downloader (start-downloader cfg app-state nntp)
              writer     (start-writer cfg app-state)
              decoder    (start-decoder cfg app-state)
              combiner   (start-segment-combiner cfg app-state)
              cleaner    (start-cleaner cfg app-state)
              ctls       (mapv :ctl [listener downloader writer decoder])]
          (async/pipe (:events watcher) (:in listener))
          (async/pipe (:out listener) (:in downloader))
          (async/pipe (:out downloader) (:in writer))
          (async/pipe (:out writer) (:in decoder))
          (async/pipe (:out decoder) (:in combiner))
          (async/pipe (:out combiner) (:in cleaner))
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

;; public

;; 1 file -> 12 segments
;; 10 nntp workers downloading putting results onto same channel for file
;; pass reply channel onto writer
;; writer takes all values off chann





;; listen for fs events
;; get an event, parse it into nzb map thing
;; start each file downloading?
;;   - put in appstate the current files
;;   - when nntp pull off work queue will update state to say "downloading"
;;   - nntp needs to update appstate with no. bytes downloaded as it goes

;; have n workers for writing segments to file
;; have n workers for decoding yenc encoded segment files into single file
;;   - only 1 worker per file though, how to prevent multiple threads writing to same randomaccessfile?
;; create pipeline of channels to take results from reply channel of download and push to decode and writing

;; status of file and segments need to be kept in sync
;; if interupted, app state will be saved
;;   - need to be able to resume current state of downloads from appstate, should be possible

;; ws component that adds watch to app-state and dumps down over websocket
;; om? lol ui that updates based on state changes to atom getting syncd by websocket

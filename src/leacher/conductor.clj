dec(ns leacher.conductor
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
  [body-fn & {:keys [workers channels]
              :or   {workers  1
                     channels {:in  (chan 100)
                               :out (chan 100)
                               :ctl (chan)}}}]
  (let [ctls (mapv chan (range workers))]
    (thread
      (<!! (:ctl channels))
      (dotimes [n workers]
        (async/close! (nth ctls n))))
    (dotimes [n workers]
      (thread
        (loop []
          (alt!!
            (:in channels)
            ([val]
               (when val
                 (try
                   (body-fn n channels val)
                   (catch Exception e
                     (log/errorf e "worker[%d]: error" n)))
                 (recur)))

            (nth ctls n)
            ([_]
               (log/infof "worker[%d]: shutdown" n))))))
    channels))

;; TODO: how do we know when we are "done" with downloading a files segments?
;; move completed file to complete dir
;; mv .nzb file to complete dir?

(defn start-cleaner
  [cfg app-state]
  (worker (fn [n channels val]
            (let [seg-file (:segment-file val)]
              (log/info "cleaning up" (str seg-file))
              (fs/delete seg-file)))))

(defn start-segment-combiner
  [cfg app-state]
  (worker (fn [_ channels val]
            (let [file (io/file (-> cfg :dirs :temp) (-> val :file :filename))]
              (io/make-parents file)
              (yenc/decode-to (:decoded-segment val) file)
              (>!! (:out channels)
                   (assoc val :decoded-file file))))))

(defn start-decoder
  [cfg app-state]
  (worker (fn [n channels val]
            (logt (format "decoder[%d]: decoding file:"
                          n (str (:segment-file val)))
              (let [seg-file (:segment-file val)
                    decoded  (yenc/decode-segment seg-file)]
                (>!! (:out channels)
                     (assoc val :decoded-segment decoded)))))
          :workers 10))

(defn start-writer
  [cfg app-state]
  (worker (fn [n channels val]
            (let [dir  (-> cfg :dirs :temp)
                  file (io/file dir (-> val :segment :message-id))]
              (logt (format "writer[%d]: writing segment to %s" n (str file))
                (io/copy (-> val :resp :bytes) file)
                (>!! (:out channels)
                     (assoc val :segment-file file)))))
          :workers 10))

(defn start-downloader
  [cfg app-state nntp]
  (worker (fn [n channels val]
            (log/info "starting download of file" (:filename val))
            (let [reply (nntp/download nntp val)]
              (async/pipe reply (:out channels) false)))))

(defn start-listener
  [cfg app-state]
  (worker (fn [n channels path]
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

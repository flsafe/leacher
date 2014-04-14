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

;; can be multiple decoders, but need to ensure that segments from same
;; file are all processed by same thread? OR reduce the written segments
;; to a single value that gets read by this?
(defn start-decoder
  [cfg app-state]
  (let [channels {:in  (chan 100)
                  :ctl (chan)}]
    (thread
      (loop []
        (alt!!
          (:in channels)
          ([v]
             (when v
               (try
                 (logt "decoding file:" (str (:segment-file v))
                   (let [file    (io/file (-> cfg :dirs :complete) (-> v :file :filename))
                         seg-file (:segment-file v)]
                     (io/make-parents file)
                     (yenc/decode-to seg-file file)))
                 (catch Exception e
                   (log/error e "failed to decode segment")))
               (recur)))

          (:ctl channels)
          ([_]
             (log/info "decoder got interupt, exiting")))))
    channels))

;; can be multiple writers safely, spin up n threads based on config
(defn start-writer
  [cfg app-state]
  (let [workers  10
        channels {:in  (chan 100)
                  :out (chan 100)
                  :ctl (chan)}
        ctls     (mapv chan (range workers))]
    (thread
      (<!! (:ctl channels))
      (doseq [ctl ctls]
        (async/close! ctl)))
    (dotimes [n workers]
      (thread
        (loop []
          (alt!!
            (:in channels)
            ([v]
               (when v
                 (try
                   (let [dir  (-> cfg :dirs :temp)
                         file (io/file dir (-> v :segment :message-id))]
                     (logt
                       (format "writer[%d]: writing segment to %s" n (str file))
                       (io/copy (-> v :resp :bytes) file)
                       (>!! (:out channels)
                            (assoc v :segment-file file))))
                   (catch Exception e
                     (log/errorf e "writer[%d]: failed to write segment" n)))
                 (recur)))

            (nth ctls n)
            ([_]
               (log/infof "writer[%d]: got interupt, exiting" n))))))
    channels))

;; single thread is ok, bound by no. of nntp workers will add files to
;; download and return immediately. pipe replies from channel returned
;; by nntp onto :out channel
(defn start-downloader
  [cfg app-state nntp]
  (let [channels {:in  (chan 100)
                  :out (chan 100)
                  :ctl (chan)}]
    (thread
      (loop []
        (alt!!
          (:in channels)
          ([file]
             (when file
               (try
                 (log/info "starting download of file" (:filename file))
                 (let [reply (nntp/download nntp file)]
                   (async/pipe reply (:out channels) false))
                 (catch Exception e
                   (log/error e "failed to download nzb files")))
               (recur)))

          (:ctl channels)
          ([_]
             (log/info "downloader got interupt, exiting")))))
    channels))

;; single thread ok, just dumping nzbs onto a channel for processing
(defn start-listener
  [cfg app-state in]
  (let [channels {:in  in
                  :out (chan)
                  :ctl (chan)}]
    (thread
      (loop []
        (alt!!
          (:in channels)
          ([f]
             (when f
               (try
                 (log/info "received nzb file:" (str f))
                 (doseq [file (nzb/parse (io/file f))]
                   (>!! (:out channels) file))
                 (catch Exception e
                   (log/error e "failed to parse nzb file")))
               (recur)))

          (:ctl channels)
          ([_]
             (log/info "listener got interupt, exiting")))))
    channels))

;; component

(defrecord Conductor [cfg app-state watcher nntp channels]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels [(start-listener cfg app-state (:events watcher))
                      (start-downloader cfg app-state nntp)
                      (start-writer cfg app-state)
                      (start-decoder cfg app-state)]
            pairs    (partition 2 1 channels)]
        (log/info "starting")
        (doseq [[from to] pairs
                :let [out (:out from)
                      in  (:in to)]]
          (async/pipe out in false))
        (assoc this :channels channels))
      this))

  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [{:keys [ctl]} channels]
          (async/close! ctl))
        (assoc this :channels nil)
      this))))

(defn new-conductor
  [cfg]
  (map->Conductor {:cfg cfg}))

;; public

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

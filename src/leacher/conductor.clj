(ns leacher.conductor
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [chan >!! <!! thread alt!!]]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
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
             (try
               (log/info "starting to decode file:" (str (:segment-file v)))
               (let [seg-file (:segment-file v)
                     file     (io/file (fs/parent seg-file) (:filename (:file v)))
                     output   (RandomAccessFile. file "rw")
                     decoded  (yenc/decode-segment seg-file)
                     begin    (-> decoded :keywords :part :begin dec)
                     end      (-> decoded :keywords :part :end dec)]
                 (.seek output begin)
                 (.write output ^bytes (:bytes decoded)))
               (catch Exception e
                 (log/error e "failed to decode segment")))
             (recur))

          (:ctl channels)
          ([_]
             (log/info "got interupt, exiting")))))
    channels))

(defn dir-for
  [file cfg]
  (let [dir (io/file (-> cfg :dirs :temp) (:filename file))]
    (fs/mkdirs dir)
    dir))

;; can be multiple writers safely, spin up n threads based on config
(defn start-writer
  [cfg app-state]
  (let [channels {:in  (chan 100)
                  :out (chan 100)
                  :ctl (chan)}]
    (thread
      (loop []
        (alt!!
          (:in channels)
          ([v]
             (try
               (log/info "received segment to write")
               (let [dir  (dir-for (:file v) cfg)
                     file (io/file dir (-> v :segment :message-id))]
                 (log/info "writing segment to" (str file))
                 (io/copy (-> v :resp :bytes) file)
                 (>!! (:out channels)
                      (assoc v :segment-file file)))
               (catch Exception e
                 (log/error e "failed to write segment")))
             (recur))

          (:ctl channels)
          ([_]
             (log/info "got interupt, exiting")))))
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
          ([nzb]
             (try
               (log/info "starting download of nzb")
               (let [replies (mapv (partial nntp/download nntp) (:files nzb))]
                 (doseq [r replies]
                   (async/pipe r (:out channels) false)))
               (catch Exception e
                 (log/error e "failed to download nzb files")))
             (recur))

          (:ctl channels)
          ([_]
             (log/info "got interupt, exiting")))))
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
             (try
               (log/info "received nzb file:" (str f))
               (let [nzb (nzb/parse (io/file f))]
                 (>!! (:out channels) nzb))
               (catch Exception e
                 (log/error e "failed to parse nzb file")))
             (recur))

          (:ctl channels)
          ([_]
             (log/info "got interupt, exiting")))))
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
            pairs (partition 2 1 channels)]
        (log/info "starting")
        (doseq [[from to] pairs
                :let [out (:out from)
                      in  (:in to)]]
          (async/pipe out in))
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

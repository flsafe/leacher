(ns leacher.conductor
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [chan >!! <!! thread alt!!]]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [leacher.utils :refer [logt]]
            [leacher.watcher :as watcher]
            [leacher.nntp :as nntp]
            [leacher.nzb :as nzb]
            [leacher.decoders.yenc :as yenc])
  (:import [java.io RandomAccessFile]))

;; component

(defrecord Conductor [cfg nzb-parser watcher nntp yenc-decoder cleaner channels]
  component/Lifecycle
  (start [this]
    (if-not channels
      (do
        (log/info "starting")
        (let [channels [(:channels watcher)
                        (:channels nzb-parser)
                        (:channels nntp)
                        (:channels yenc-decoder)
                        (:channels cleaner)]]
          (doseq [[c1 c2] (partition 2 1 channels)
                  :let [from (:out c1)
                        to   (:in c2)]]
            (async/pipe from to))
          (assoc this :channels channels)))
      this))

  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [chs channels
                [_ ch] chs]
          (when ch
            (async/close! ch)))
        (assoc this :channels nil))
      this)))

(defn new-conductor
  [cfg]
  (map->Conductor {:cfg cfg}))

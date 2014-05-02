(ns leacher.conductor
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]))

;; Some components have a map of channels, typically containing :in,
;; :out, :ctl. This component wires these together in the appropriate
;; order.

;; component

(defrecord Conductor [cfg parser watcher downloader decoder cleaner channels]
  component/Lifecycle
  (start [this]
    (if-not channels
      (do
        (log/info "starting")
        (let [channels [(:channels watcher)
                        (:channels parser)
                        (:channels downloader)
                        (:channels decoder)
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

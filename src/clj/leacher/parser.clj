(ns leacher.parser
  (:require [clojure.core.async :as async :refer [<! >! chan go-loop]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.state :as state]
            [leacher.nzb :as nzb]))

;; component

(defn start-listening
  [{:keys [in out]} app-state]
  (go-loop []
    (if-let [f (<! in)]
      (let [files (nzb/parse f)]
        (doseq [[filename src-file] files
                :let [file (state/new-scope app-state :downloads filename)]
                :when (not @file)]
          (state/reset! file (assoc src-file :status :waiting))
          (log/info "sending file to downloader" (:filename @file))
          (>! out file))
        (fs/delete f)
        (recur))
      (log/info "exiting"))))

(defrecord Parser [channels app-state]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in  (chan)
                      :out (chan)}]
        (start-listening channels app-state)
        (assoc this :channels channels))
      this))

  (stop [this]
    (if channels
      (do
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels nil))
      this)))

(defn new-parser
  []
  (map->Parser {}))

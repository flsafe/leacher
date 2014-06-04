(ns leacher.parser
  (:require [clojure.core.async :as async :refer [<! >! chan go-loop]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.state :as state]
            [leacher.nzb :as nzb]))

;; component

(defn start-listening
  [{:keys [watcher parser]} app-state]
  (go-loop []
    (if-let [f (<! watcher)]
      (let [files (nzb/parse f)]
        (doseq [[filename src-file] files
                :let [file (state/new-scope app-state :downloads filename)]
                :when (not @file)]
          (state/reset! file (assoc src-file :status :waiting))
          (log/info "putting" filename "on parser channel")
          (>! parser file))
        (fs/delete f)
        (recur))
      (log/info "exiting"))))

(defrecord Parser [channels app-state process]
  component/Lifecycle
  (start [this]
    (if-not process
      (let [process (start-listening channels app-state)]
        (assoc this :process process))
      this))

  (stop [this]
    (if process
      (assoc this :process nil)
      this)))

(defn new-parser
  [channels]
  (map->Parser {:channels channels}))

(ns leacher.watcher
  (:require [clojure.core.async :as async :refer [>! alt! chan
                                                  go-loop]]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.config :as config]))

(defn start-watching
  [{:keys [watcher shutdown]}]
  (let [glob (str config/queue-dir "/*.nzb")]
    (go-loop [previous #{}]
      (alt!
        shutdown
        ([_]
           (log/info "shutting down"))

        (async/timeout 1000)
        ([_]
           (let [current   (set (fs/glob glob))
                 new-files (set/difference current previous)]
             (when-not (empty? new-files)
               (doseq [f new-files]
                 (log/info "putting new file" f "on watcher chan")
                 (>! watcher f)))
             (recur current)))

        :priority true))))

;; component

(defrecord Watcher [channels worker]
  component/Lifecycle
  (start [this]
    (if-not worker
      (do (log/info "starting")
          (assoc this :worker (start-watching channels)))
      this))

  (stop [this]
    (if worker
      (do (log/info "stopping")
          (assoc this :worker nil))
      this)))

(defn new-watcher
  []
  (map->Watcher {}))

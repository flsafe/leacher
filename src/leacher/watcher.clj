(ns leacher.watcher
  (:require [me.raynes.fs :as fs]
            [leacher.config :as cfg]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.core.async :as async :refer [thread <!! >!! chan alt!!]]))

(defn start-watching
  [dir events ctl]
  (let [glob (str dir "/*.nzb")]
    (thread
      (loop [previous #{}]
        (alt!!
          (async/timeout 1000)
          ([_]
             (let [current   (set (fs/glob glob))
                   new-files (set/difference current previous)]
               (when-not (empty? new-files)
                 (doseq [f new-files]
                   (>!! events f)))
               (recur current)))

          ctl
          ([_]
             (log/info "got shutdown signal")))))))

;; component

(defrecord Watcher [dir events ctl app-state]
  component/Lifecycle
  (start [this]
    (if-not events
      (let [events (chan)
            ctl    (chan)]
        (log/info "starting to watch" dir)
        (start-watching dir events ctl)
        (assoc this :events events :ctl ctl))
      this))

  (stop [this]
    (if events
      (do
        (log/info "stopping")
        (async/close! ctl)
        (async/close! events)
        (assoc this :events nil :ctl nil))
      this)))

(defn new-watcher
  [dir]
  (map->Watcher {:dir dir}))

(comment
  (def w (component/start (new-watcher "/home/gareth/.leacher/queue")))
  (component/stop w)
  (async/close! (:ctl w))
  (thread
    (loop []
      (when-let [f (<!! (:ch w))]
        (println f)
        (recur))))

  )

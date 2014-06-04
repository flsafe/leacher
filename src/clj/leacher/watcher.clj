(ns leacher.watcher
  (:require [clojure.core.async :as async :refer [>! alt! chan
                                                  go-loop]]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]))

(defn start-watching
  [dir {:keys [watcher]} ctl]
  (let [glob (str dir "/*.nzb")]
    (go-loop [previous #{}]
      (alt!
        (async/timeout 1000)
        ([_]
           (let [current   (set (fs/glob glob))
                 new-files (set/difference current previous)]
             (when-not (empty? new-files)
               (doseq [f new-files]
                 (log/info "putting new file" f "on watcher chan")
                 (>! watcher f)))
             (recur current)))

        ctl
        ([_]
           (log/debug "exiting"))))))

;; component

(defrecord Watcher [dir channels ctl]
  component/Lifecycle
  (start [this]
    (if-not ctl
      (let [ctl (chan)]
        (start-watching dir channels ctl)
        (assoc this :ctl ctl))
      this))

  (stop [this]
    (if ctl
      (do
        (async/close! (:watcher channels))
        (async/close! ctl)
        (assoc this :ctl nil))
      this)))

(defn new-watcher
  [dir channels]
  (map->Watcher {:dir      dir
                 :channels channels}))

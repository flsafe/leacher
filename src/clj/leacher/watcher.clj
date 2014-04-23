(ns leacher.watcher
  (:require [clojure.core.async :as async :refer [>!! alt!! chan
                                                  thread]]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]))

(defn start-watching
  [dir {:keys [out ctl]}]
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
                   (log/info "putting new file" f "on out chan")
                   (>!! out f)))
               (recur current)))

          ctl
          ([_]
             (log/debug "exiting")))))))

;; component

(defrecord Watcher [dir channels app-state]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:out (chan)
                      :ctl (chan)}]
        (log/info "starting with" dir)
        (start-watching dir channels)
        (assoc this :channels channels))
      this))

  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels nil))
      this)))

(defn new-watcher
  [dir]
  (map->Watcher {:dir dir}))

(ns leacher.ui.ws
  (:require [org.httpkit.server :refer [run-server with-channel send! on-close on-receive]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [leacher.state :as state]))

;; ws component

(defn state-event
  [state]
  (pr-str {:type :server-state
           :data state}))

(defn ws-handler
  [clients app-state req]
  (with-channel req channel
    (log/info "client connected from" (:remote-addr req))
    (swap! clients conj channel)
    (send! channel (state-event (state/state app-state)))
    (on-close channel
              (fn [status]
                (log/info "client from" (:remote-addr req) "disconnected")
                (swap! clients disj channel)))))

(defn on-update
  [clients key ref old new]
  (let [to-send (state-event new)]
    (doseq [ch @clients]
      (send! ch to-send))))

(defrecord WsApi [cfg app-state clients stop-server-fn]
  component/Lifecycle
  (start [this]
    (if stop-server-fn
      this
      (let [clients        (atom #{})
            _              (log/info "starting with" cfg)
            stop-server-fn (run-server (partial ws-handler clients app-state) cfg)]
        (log/info "starting")
        ;; TODO perhaps introduce sliding buffer between atom and
        ;; publishing to prevent too many client updates
        (state/watch app-state :ws-watch (partial on-update clients))
        (assoc this
          :stop-server-fn stop-server-fn
          :clients clients))))

  (stop [this]
    (if stop-server-fn
      (do
        (log/info "stopping")
        (state/stop-watching app-state :ws-watch)
        (stop-server-fn)
        (reset! clients (atom #{}))
        (assoc this :stop-server-fn nil :clients nil))
      this)))

(defn new-ws-api
  [cfg]
  (map->WsApi {:cfg cfg}))

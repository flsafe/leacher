(ns leacher.ui.ws
  (:require [org.httpkit.server :refer [run-server with-channel send! on-close on-receive close]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [leacher.state :as state]
            [clojure.core.async :as async :refer [thread <!! >!! sliding-buffer chan]]))

;; ws component

(defn state-event
  [state]
  (pr-str {:type :server-state
           :data (update-in state [:downloads]
                   (fn [files]
                     (reduce-kv #(assoc %1 %2 (dissoc %3 :segments)) {} files)))}))

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
  [ch key ref old new]
  (async/put! ch new))

(defn start-publisher
  [clients ch]
  (thread
    (loop []
      (when-let [v (<!! ch)]
        (let [to-send (state-event v)]
          (doseq [ch @clients]
            (send! ch to-send)))
        ;; woah there
        (Thread/sleep 100)
        (recur)))))

(defrecord WsApi [cfg app-state clients stop-server-fn events ch]
  component/Lifecycle
  (start [this]
    (if stop-server-fn
      this
      (let [clients        (atom #{})
            ch             (chan (sliding-buffer 1))
            stop-server-fn (run-server (partial ws-handler clients app-state) cfg)]
        (log/info "starting")
        (start-publisher clients ch)
        (state/watch app-state :ws-watch (partial on-update ch))
        (assoc this
          :stop-server-fn stop-server-fn
          :clients clients
          :ch ch))))

  (stop [this]
    (if stop-server-fn
      (do
        (log/info "stopping")
        (state/stop-watching app-state :ws-watch)
        (doseq [c @clients]
          (close c))
        (async/close! ch)
        (stop-server-fn)
        (reset! clients (atom #{}))
        (assoc this :stop-server-fn nil :clients nil :ch nil))
      this)))

(defn new-ws-api
  [cfg]
  (map->WsApi {:cfg cfg}))

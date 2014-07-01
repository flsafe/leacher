(ns leacher.ws
  (:require [clojure.core.async :as async :refer [<!! chan thread put! alt!!]]
            [clojure.data :as data]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [leacher.nntp :as nntp]
            [leacher.settings :as stg]
            [org.httpkit.server :refer [close on-close run-server send!
                                        with-channel on-receive]]))

;; events

(defn state-event
  [data]
  (pr-str {:type :initial
           :data data}))

(defmulti merge-state (fn [_ e] (:type e)))

(defmethod merge-state :download-starting
  [state {:keys [file]}]
  (swap! state assoc (:filename file)
    (assoc file :status :downloading)))

(defmethod merge-state :default
  [_ e]
  (log/info "unhandled event" (:type e)))

(defn start-publisher
  [clients state {:keys [events shutdown]}]
  (thread
    (loop []
      (alt!!
        shutdown
        ([_]
           (log/info "shutting down"))

        events
        ([event]
           (when event
             (try
               (merge-state state event)
               (doseq [ch @clients]
                 (send! ch event))
               (catch Exception e
                 (log/error e "failed sending to client")))
             (recur)))

        :priority true))))

(defn settings-update
  [settings new-settings]
  (try
    (log/info "updating settings to" new-settings)
    (stg/merge-with! settings new-settings)
    (catch Exception e
      (log/error e "failed updating settings"))))

(defn ws-handler
  [clients state settings req]
  (with-channel req channel
    (log/info "client connected from" (:remote-addr req))
    (swap! clients conj channel)
    (send! channel (state-event @state))
    (on-receive channel
      (fn [data]
        (let [msg (edn/read-string data)]
          (case (:type msg)
            :settings-update (settings-update settings (:settings msg))))))

    (on-close channel
      (fn [status]
        (log/info "client from" (:remote-addr req) "disconnected")
        (swap! clients disj channel)))))

;; component

(defrecord WsApi [settings clients stop-server-fn channels state]
  component/Lifecycle
  (start [this]
    (if stop-server-fn
      this
      (let [clients        (atom #{})
            state          (atom {})
            stop-server-fn (run-server #(ws-handler clients state settings %) {:port 8091})]
        (log/info "starting")
        (start-publisher clients state channels)
        (assoc this
          :stop-server-fn stop-server-fn
          :clients clients
          :state state))))

  (stop [this]
    (if stop-server-fn
      (do
        (log/info "stopping")
        (doseq [c @clients]
          (close c))
        (stop-server-fn)
        (reset! clients (atom #{}))
        (assoc this
          :stop-server-fn nil
          :clients nil
          :state state))
      this)))

(defn new-ws-api
  []
  (map->WsApi {}))

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

(defn initial-event
  [files settings]
  (pr-str {:type :initial
           :data {:files    files
                  :settings settings}}))

(defn merge-state
  [state {:keys [type filename file status] :as data}]
  (case type
    :download-pending
    (swap! state assoc (:filename file)
      (-> file
        (assoc :status :pending
               :downloaded-segments 0
               :decoded-segments 0
               :download-failed-segments 0
               :decode-failed-segments 0
               :errors [])
        (dissoc :segments)))

    :file-status
    (swap! state assoc-in [filename :status] status)

    :segment-download-complete
    (swap! state update-in [filename :downloaded-segments] inc)

    :segment-download-failed
    (swap! state update-in [filename]
      (fn [m] (-> m
                (update-in [:download-failed-segments] inc)
                (update-in [:errors] conj (:message data)))))

    :segment-decode-complete
    (swap! state update-in [filename :decoded-segments] inc)

    :segment-decode-failed
    (swap! state update-in [filename]
      (fn [m] (-> m
                (update-in [:decode-failed-segments] inc)
                (update-in [:errors] conj (:message data)))))))

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
               (let [ws-event (pr-str {:type :update
                                       :data event})]
                 (doseq [ch @clients]
                   (send! ch ws-event)))
               (catch Exception e
                 (log/error e "failed sending to client")))
             (recur)))

        :priority true))))

(defn ws-handler
  [clients state settings req]
  (with-channel req channel
    (log/info "client connected from" (:remote-addr req))
    (swap! clients conj channel)
    (send! channel (initial-event @state (stg/all settings)))

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
        (assoc this
          :stop-server-fn nil
          :clients nil
          :state nil))
      this)))

(defn new-ws-api
  []
  (map->WsApi {}))

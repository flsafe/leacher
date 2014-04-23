(ns leacher.ui.ws
  (:require [org.httpkit.server :refer [run-server with-channel send! on-close on-receive close]]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [leacher.state :as state]
            [clojure.data :as data]
            [clojure.core.async :as async :refer [thread <!! >!! sliding-buffer chan]]))

;; remove segments from state sent to client, not needed and large
;; amount of data
(defn without-segments
  [files]
  (reduce-kv #(assoc %1 %2 (dissoc %3 :segments)) {} files))

(defn paths-from
  "Turns a map into a vector of paths to all the keys contained within
  the map. Used to delete keys from state held by client on a delta
  update."
  [m]
  (reduce-kv (fn [res k v]
               (if (map? v)
                 (conj res (into [k] (paths-from v)))
                 (conj res k))) [] m))

(defn deltas-between
  [old new]
  (let [old            (update-in old [:downloads] without-segments)
        new            (update-in new [:downloads] without-segments)
        [remove add _] (data/diff old new)]
    {:remove (paths-from remove)
     :modify add}))

;; events

(defn state-event
  [state]
  (pr-str {:type :initial
           :data (update-in state [:downloads] without-segments)}))

(defn delta-event
  [old new]
  (pr-str {:type :deltas
           :data (deltas-between old new)}))

(defn on-update
  [ch key ref old new]
  (async/put! ch (delta-event old new)))

(defn start-publisher
  [clients ch]
  (thread
    (loop []
      (when-let [v (<!! ch)]
        (doseq [ch @clients]
          (send! ch v))
        ;; woah there
        (Thread/sleep 100)
        (recur)))))

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

;; component

(defrecord WsApi [cfg app-state clients stop-server-fn events ch]
  component/Lifecycle
  (start [this]
    (if stop-server-fn
      this
      (let [clients        (atom #{})
            ch             (chan (sliding-buffer 10))
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

(ns leacher.ui.ws
  (:require [clojure.core.async :as async :refer [<!! chan thread put!]]
            [clojure.data :as data]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.state :as state]
            [org.httpkit.server :refer [close on-close run-server send!
                                        with-channel on-receive]]))

;; remove segments from state sent to client, not needed and large
;; amount of data
(defn without-segments
  [files]
  (reduce-kv #(assoc %1 %2 (dissoc %3 :segments)) {} files))

(defn paths-from
  [m]
  (reduce-kv (fn [res k v]
               (if (map? v)
                 (conj res [k (paths-from v)])
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
  [{:keys [work]} key ref old new]
  (async/put! work (delta-event old new)))

(defn start-publisher
  [clients {:keys [work]}]
  (thread
    (loop []
      (when-let [v (<!! work)]
        (doseq [ch @clients]
          (send! ch v))
        (recur)))))

(defn ws-handler
  [clients app-state {:keys [cancels]} req]
  (with-channel req channel
    (log/info "client connected from" (:remote-addr req))
    (swap! clients conj channel)
    (send! channel (state-event (state/state app-state)))
    (on-receive channel
      (fn [data]
        (case (:type data)
          :clear-completed (state/clear-completed! app-state)
          :cancel-all (do
                        (state/cancel-all! app-state)
                        (put! cancels {:type :all})))))

    (on-close channel
      (fn [status]
        (log/info "client from" (:remote-addr req) "disconnected")
        (swap! clients disj channel)))))

;; component

(defrecord WsApi [cfg app-state clients stop-server-fn events channels]
  component/Lifecycle
  (start [this]
    (if stop-server-fn
      this
      (let [clients        (atom #{})
            channels       {:work    (chan)
                            :cancels (chan)}
            stop-server-fn (run-server (partial ws-handler clients app-state channels) cfg)]
        (log/info "starting")
        (start-publisher clients channels)
        (state/watch app-state :ws-watch (partial on-update channels))
        (assoc this
          :stop-server-fn stop-server-fn
          :clients clients
          :channels channels))))

  (stop [this]
    (if stop-server-fn
      (do
        (log/info "stopping")
        (state/stop-watching app-state :ws-watch)
        (doseq [c @clients]
          (close c))
        (doseq [ch (vals channels)]
          (async/close! ch))
        (stop-server-fn)
        (reset! clients (atom #{}))
        (assoc this
          :stop-server-fn nil
          :clients nil
          :channels nil))
      this)))

(defn new-ws-api
  [cfg]
  (map->WsApi {:cfg cfg}))

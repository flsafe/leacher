(ns leacher.ui.http
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [com.stuartsierra.component :as component]
            [compojure.core :refer [routes GET context]]
            [org.httpkit.server :as server]
            [ring.util.response :refer [response header]]
            [leacher.state :as state]))

(defn state-response
  [app-state]
  (-> (json/write-str (state/get-state app-state))
      response
      (header "content-type" "application/json")))

(defn build-routes
  [app-state]
  (routes (GET "/state" [] (state-response app-state))))

;; component

(defrecord HttpServer [cfg app-state shutdown-fn]
  component/Lifecycle
  (start [this]
    (if shutdown-fn
      this
      (let [shutdown-fn (server/run-server (build-routes app-state) cfg)]
        (log/info "starting")
        (assoc this :shutdown-fn shutdown-fn))))
  (stop [this]
    (if-not shutdown-fn
      this
      (do
        (log/info "stopping")
        (shutdown-fn)
        (assoc this :shutdown-fn nil)))))

(defn new-http-server
  [cfg]
  (map->HttpServer {:cfg cfg}))

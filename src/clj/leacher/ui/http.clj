(ns leacher.ui.http
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [com.stuartsierra.component :as component]
            [compojure.core :refer [routes GET context]]
            [compojure.handler :refer [site]]
            [hiccup.page :refer [html5 include-js include-css]]
            [org.httpkit.server :as server]
            [ring.util.response :refer [response header]]
            [ring.middleware.resource :as ring-resource]
            [ring.middleware.stacktrace :as ring-stacktrace]
            [ring.middleware.reload :as ring-reload]
            [leacher.state :as state]))

(defn index-page
  [cfg]
  (html5
   [:head
    (include-css
      "css/bootstrap.min.css"
      "css/site.css")]
   [:body.base3
    [:div#app]
    (include-js
      "js/react.js"
      "js/moment.js"
      "js/app.js")
    [:script
     "leacher.ui.app.init('" (pr-str cfg) "');"]]))

(defn build-routes
  [cfg]
  (GET "/" [] (index-page cfg)))

(defn build-app
  [cfg]
  (-> (build-routes cfg)
      site
      (ring-resource/wrap-resource "public")
      ring-stacktrace/wrap-stacktrace
      ring-reload/wrap-reload))

;; component

(defrecord HttpServer [cfg shutdown-fn]
  component/Lifecycle
  (start [this]
    (if shutdown-fn
      this
      (let [shutdown-fn (server/run-server (build-app cfg) (:http-server cfg))]
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

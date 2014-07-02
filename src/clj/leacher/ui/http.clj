(ns leacher.ui.http
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [compojure.core :refer [GET]]
            [compojure.handler :refer [site]]
            [hiccup.page :refer [html5 include-css include-js]]
            [org.httpkit.server :as server]
            [ring.middleware.reload :as ring-reload]
            [ring.middleware.resource :as ring-resource]
            [ring.middleware.stacktrace :as ring-stacktrace]))

(defn index-page
  []
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
     "leacher.ui.app.init();"]]))

(defn build-routes
  []
  (GET "/" [] (index-page)))

(defn build-app
  []
  (-> (build-routes)
      site
      (ring-resource/wrap-resource "public")
      ring-stacktrace/wrap-stacktrace
      ring-reload/wrap-reload))

;; component

(defrecord HttpServer [shutdown-fn]
  component/Lifecycle
  (start [this]
    (if shutdown-fn
      this
      (let [shutdown-fn (server/run-server (build-app) {})]
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
  []
  (map->HttpServer {}))

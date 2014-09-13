(ns user
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.namespace.repl :refer (refresh)]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.core.async :as async :refer [put!]]
            [leacher.main :as app]
            [leacher.config :as cfg]))

(defonce system nil)

(defn init []
  (alter-var-root #'system
    (constantly (app/new-leacher-system)))
  nil)

(defn start []
  (alter-var-root #'system component/start)
  nil)

(defn stop []
  (alter-var-root #'system
    (fn [s] (when s (component/stop s))))
  nil)

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))

(comment
  (go)
  (start)
  (reset)
  (init)
  (stop)


  )

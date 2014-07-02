(ns leacher.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan close!]]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [leacher.utils :refer [on-shutdown]]
            [leacher.config :as config]
            [leacher.pipeline :as pipeline]
            [leacher.downloader :as downloader]
            [leacher.decoder :as decoder]
            [leacher.settings :as settings]
            [leacher.watcher :as watcher]
            [leacher.ws :as ws]
            [leacher.ui.http :as http])
  (:gen-class))

;; system

(defrecord Channels [watcher downloads decodes events shutdown]
  component/Lifecycle
  (start [this]
    (assoc this
      :watcher (chan)
      :downloads (chan)
      :decodes (chan)
      :events (chan)
      :shutdown (chan)))
  (stop [this]
    (close! watcher)
    (close! downloads)
    (close! decodes)
    (close! events)
    (close! shutdown)
    (assoc this
      :watcher nil
      :downloads nil
      :decodes nil
      :events nil
      :shutdown nil)))

(defrecord LeacherSystem []
  component/Lifecycle
  (start [this]
    (log/info "starting leacher")
    (component/start-system this))
  (stop [this]
    (log/info "stopping leacher")
    (component/stop-system this)))

(defn new-leacher-system
  []
  (map->LeacherSystem
    {:channels   (map->Channels {})

     :settings   (settings/new-settings)

     :watcher    (component/using (watcher/new-watcher)
                   [:channels])

     :pipeline   (component/using (pipeline/new-pipeline)
                   [:channels])

     :downloader (component/using (downloader/new-downloader)
                   [:channels :settings])

     :decoder    (component/using (decoder/new-decoder)
                   [:channels])

     :ws         (component/using (ws/new-ws-api)
                   [:channels :settings])

     :http       (http/new-http-server)}))

;; entry point

(def options-spec
  [["-h" "--help"    "Show this message"]
   ["-e" "--env ENV" "Environment to run in"
    :default :dev :parse-fn keyword]])

(defn -main
  [& args]
  (let [opts    (parse-opts args options-spec)
        options (:options opts)]

    (when-let [errors (:errors opts)]
      (println "Error: Bad argument(s)")
      (doseq [error errors]
        (println error))
      (println "Usage:\n\n" (:summary opts) "\n")
      (System/exit 1))

    (when (:help options)
      (println (:summary opts))
      (System/exit 0))

    (when-not (fs/exists? config/home-dir)
      (println "setting up leacher home directory" config/home-dir)
      (fs/mkdir config/home-dir))

    (let [system (component/start (new-leacher-system))]
      (on-shutdown
        (log/info "interrupted! shutting down")
        (component/stop system))
      (.join (Thread/currentThread)))))

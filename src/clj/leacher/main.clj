(ns leacher.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan]]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [leacher.utils :refer [on-shutdown]]
            [leacher.config :as config]
            [leacher.pipeline :as pipeline]
            [leacher.downloader :as downloader]
            [leacher.decoder :as decoder]
            [leacher.settings :as settings]
            [leacher.watcher :as watcher])
  (:gen-class))

;; system

(def components
  [:settings
   :pipeline
   :downloader
   :decoder
   :watcher])

(defrecord LeacherSystem []
  component/Lifecycle
  (start [this]
    (log/info "starting leacher")
    (component/start-system this components))
  (stop [this]
    (log/info "stopping leacher")
    (component/stop-system this components)))

(defn new-leacher-system
  []
  (map->LeacherSystem
    {:channels   {:watcher   (chan 20)
                  :downloads (chan 500)
                  :decodes   (chan 500)
                  :shutdown  (chan)}

     :settings   (settings/new-settings)

     :watcher    (component/using (watcher/new-watcher)
                   [:channels])

     :pipeline   (component/using (pipeline/new-pipeline)
                   [:channels])

     :downloader (component/using (downloader/new-downloader)
                   [:channels :settings])

     :decoder    (component/using (decoder/new-decoder)
                   [:channels])}))

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

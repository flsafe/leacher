(ns leacher.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan]]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [leacher.config :as config]
            [leacher.nntp :as nntp]
            [leacher.state :as state])
  (:gen-class))

;; shutdown hooks

(defn add-shutdown-hook
  [f]
  (.addShutdownHook (java.lang.Runtime/getRuntime)
                    (Thread. ^Runnable f)))

(defmacro on-shutdown
  [& body]
  `(add-shutdown-hook (fn [] ~@body)))

;; system

(def components
  [:app-state
   :nntp])

(defrecord LeacherSystem [cfg]
  component/Lifecycle
  (start [this]
    (log/info "starting leacher")
    (component/start-system this components))
  (stop [this]
    (log/info "stopping leacher")
    (component/stop-system this components)))

(defn new-leacher-system
  [cfg]
  (map->LeacherSystem
   {:cfg       cfg
    :app-state (state/new-app-state (:app-state cfg))
    :nntp      (component/using (nntp/new-nntp (:nntp cfg))
                                [:app-state])}))

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
      (fs/mkdir config/home-dir)
      (println "creating template config file")
      (let [config-file (fs/file config/home-dir "config.edn")]
        (spit config-file (pr-str config/template))
        (println "please edit" (fs/absolute-path config-file))
        (System/exit 0)))

    (let [config-file (fs/file config/home-dir "config.edn")
          cfg         (edn/read-string (slurp (io/reader config-file)))
          _           (log/info "starting with" cfg)
          system      (component/start (new-leacher-system cfg))]
      (on-shutdown
        (log/info "interrupted! shutting down")
        (component/stop system))
      (.join (Thread/currentThread)))))

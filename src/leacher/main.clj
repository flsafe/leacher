(ns leacher.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan]]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [leacher.config :as cfg]
            [leacher.nntp :as nntp])
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
  [:nntp])

(defrecord LeacherSystem [config]
  component/Lifecycle
  (start [this]
    (log/info "starting leacher")
    (component/start-system this components))
  (stop [this]
    (log/info "stopping leacher")
    (component/stop-system this components)))

(defn new-leacher-system
  [config]
  (let [work-chan (chan 100)]
   (map->LeacherSystem
     {:config config
      :nntp   (nntp/new-nntp (:nntp config) work-chan)})))

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

    (when-not (fs/exists? cfg/home-dir)
      (println "setting up leacher home directory" cfg/home-dir)
      (fs/mkdir cfg/home-dir)
      (println "creating template config file")
      (let [config-file (fs/file cfg/home-dir "config.edn")]
        (spit config-file (pr-str cfg/template))
        (println "please edit" (fs/absolute-path config-file))
        (System/exit 0)))

    (let [config-file (fs/file cfg/home-dir "config.edn")
          config      (edn/read-string (slurp (io/reader config-file)))
          system      (component/start (new-leacher-system config))]
      (on-shutdown
        (log/info "interrupted! shutting down")
        (component/stop system))
      (.join (Thread/currentThread)))))

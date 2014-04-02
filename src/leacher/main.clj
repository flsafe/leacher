(ns leacher.main
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]
            [com.stuartsierra.component :as component]
            [leacher.config :as cfg])
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
  [])

(defrecord LeacherSystem [config]
  component/Lifecycle
  (start [this]
    (println "starting leacher")
    (component/start-system components))
  (stop [this]
    (println "stopping leacher")
    (component/stop-system components)))

(defn new-leacher-system
  [config]
  (map->LeacherSystem {:config config}))

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
          config      (edn/read (java.io.PushbackReader. (io/reader config-file)))
          system      (component/start (new-leacher-system config))]
      (println "starting with" config)
      (on-shutdown
        (component/stop system)))))

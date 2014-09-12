(ns leacher.settings
  (:require [com.stuartsierra.component :as component]
            [leacher.config :as config]
            [clojure.string :as string]
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]))

(def template
  {:type            :real
   :host            ""
   :port            119
   :ssl?            false
   :user            ""
   :password        ""
   :max-connections 10})

(def parsers
  {:type            keyword
   :port            #(Long. ^String %)
   :ssl?            #(= "true" %)
   :max-connections #(Long. ^String %)})

;; TODO not threadsafe
(defn write-settings
  [m]
  (let [f (fs/file config/settings-file)]
    (with-open [w ^java.io.BufferedWriter (io/writer f)]
      (doseq [[k v] m]
        (.write w (str (name k) ": " v))
        (.newLine w)))))

(defn ensure-settings
  []
  (when-not (fs/exists? config/settings-file)
    (fs/mkdir (fs/parent config/settings-file))
    (fs/create (fs/file config/settings-file))
    (write-settings template)))

(defn read-settings
  []
  (ensure-settings)
  (-> (slurp config/settings-file)
    (string/split #"\n")
    (->> (map #(string/split % #":"))
      (reduce #(assoc %1 (keyword (first %2))
                      ((get parsers (keyword (first %2)) identity)
                       (string/trim (last %2)))) {}))))

(defrecord Settings [state]
  component/Lifecycle
  (start [this]
    (if-not state
      (let [state (atom (read-settings))]
        (add-watch state :write-changes
          (fn [_ _ _ new]
            (write-settings new)))
        (assoc this :state state))
      this))
  (stop [this]
    (if state
      (do (remove-watch state :write-changes)
          (assoc this :state nil))
      this)))

(defn new-settings
  []
  (map->Settings {}))

;;

(defn merge-with!
  [settings new]
  (swap! (:state settings) merge new))

(defn get-setting
  [settings key]
  (get @(:state settings) key))

(defn all
  [settings]
  @(:state settings))

(comment
  
  (let [s (component/start (new-settings))]
    (println s)
    (component/stop s))

  )

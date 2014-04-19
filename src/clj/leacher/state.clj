(ns leacher.state
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go-loop <! >!]]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader]))

(comment

  {:downloads {"a.zip"        {:status :waiting
                               :segments {"" {}}}
               "somefile.mp3" {:status :downloading
                               :segments {"<23b@asdf.com>" {}}}}
   :workers   [{:status :waiting}
               {:status     :downloading
                :file       "somefile.mp3"
                :message-id "<23b@asdf.com>"}]}

  )

(defn read-state
  [cfg]
  (try
    (with-open [r (PushbackReader. (io/reader (:path cfg)))]
      (edn/read r))
    (catch Exception e
      (log/error e "error while reading state")
      {})))

(defn write-state
  [cfg state]
  (try
    (let [file (io/file (:path cfg))]
      (io/make-parents file)
      (spit file (pr-str @state)))
    (catch Exception e
      (log/error e "error while writing state"))))

(defrecord AppState [cfg state]
  component/Lifecycle
  (start [component]
    (if-not state
      (do
        (log/info "starting")
        (let [state (atom (read-state cfg))]
          (assoc component :state state)))
      component))

  (stop [component]
    (if state
      (do
        (log/info "stopping")
        (write-state cfg state)
        (assoc component :state nil))
      component)))

(defn new-app-state
  [cfg]
  (map->AppState {:cfg cfg}))

;; public

(defn state
  [app-state]
  @(:state app-state))

(defn watch
  [app-state key fn]
  (add-watch (:state app-state) key fn))

(defn stop-watching
  [app-state key]
  (remove-watch (:state app-state) key))

(defn get-state
  [app-state & ks]
  (get-in (state app-state) ks))

(defn set-state!
  [app-state f & args]
  (apply swap! (:state app-state) f args))

(defn set-worker!
  [app-state n & {:as data}]
  (set-state! app-state assoc-in [:workers n] data))

(defn set-file!
  [app-state filename file]
  (set-state! app-state assoc-in [:downloads filename] file))

(defn update-file!
  [app-state filename f & args]
  (apply set-state! app-state update-in
    [:downloads filename] f args))

(defn update-segment!
  [app-state filename message-id f & args]
  (apply set-state! app-state update-in
    [:downloads filename :segments message-id] f args))

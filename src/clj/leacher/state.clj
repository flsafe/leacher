(ns leacher.state
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs])
  (:import (java.io PushbackReader)))

;; don't try and persist the decoded byte arrays!
(defmethod print-method (type (byte-array 0))
  [b ^java.io.Writer w]
  (.write w "nil"))

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
      ;; we don't care about persisting worker status
      (spit file (pr-str (dissoc @state :workers))))
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
  [cfg events]
  (map->AppState {:cfg    cfg
                  :events events}))

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
  (try
    (apply swap! (:state app-state) f args)
    (catch Exception e
      (log/error e "failed to set state"))))

;; downloads state

(defn get-downloads
  [app-state]
  (get-state app-state :downloads))

(defn get-file
  [app-state filename]
  (get-state app-state :downloads filename))

(defn set-file!
  [app-state filename file]
  (set-state! app-state assoc-in [:downloads filename] file))

(defn update-file!
  [app-state filename f & args]
  (apply set-state! app-state update-in
    [:downloads filename] f args))

(defn get-segment
  [app-state filename message-id]
  (get-state app-state :downloads filename :segments message-id))

(defn update-segment!
  [app-state filename message-id f & args]
  (apply set-state! app-state update-in
    [:downloads filename :segments message-id] f args))

(defn clear-completed!
  [app-state]
  (set-state! app-state update-in [:downloads]
    (fn [m]
      (reduce-kv (fn [res filename file]
                   (if (= :completed (:status file))
                     res
                     (assoc res filename file)))
        {} m))))

(defn cancel-values
  [res k v]
  (assoc res k (assoc v :cancelled true)))

(defn cancel-file
  [file]
  (-> file
    (assoc :cancelled true
           :segments (reduce-kv cancel-values {} (:segments file)))))

(defn cancel-all!
  [app-state]
  (set-state! app-state update-in [:downloads]
    (fn [m]
      (into {}
        (for [[filename file] m]
          [filename (cancel-file file)])))))

;; worker state

(defn set-worker!
  [app-state n & {:as data}]
  (set-state! app-state assoc-in [:workers n] data))

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

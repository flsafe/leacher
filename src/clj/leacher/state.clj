(ns leacher.state
  (:refer-clojure :rename {swap! core-swap!}
                  :exclude [assoc! reset!])
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
    (apply core-swap! (:state app-state) f args)
    (catch Exception e
      (log/error e "failed to set state"))))

;; scoped data

(defrecord Scope [app-state path]
  clojure.lang.IDeref
  (deref [this]
    (get-in @(:state app-state) path)))

(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
(prefer-method print-method clojure.lang.IRecord clojure.lang.IPersistentMap)
(prefer-method print-method clojure.lang.IDeref clojure.lang.IPersistentMap)

(defn new-scope
  [from & path]
  (let [[app-state path] (if (instance? Scope from)
                           [(:app-state from) (into (:path from) path)]
                           [from path])]
    (map->Scope {:app-state app-state :path (vec path)})))

(defn reset!
  [scope val]
  (set-state! (:app-state scope) assoc-in (:path scope) val))

(defn assoc!
  [scope key val & kvs]
  (set-state! (:app-state scope) update-in (:path scope)
    (fn [m]
      (apply assoc m key val kvs))))

(defn assoc-in!
  [scope ks v]
  (set-state! (:app-state scope) assoc-in (into (:path scope) ks) v))

(defn update-in!
  [scope ks f & args]
  (apply set-state! (:app-state scope) update-in (into (:path scope) ks) f args))

(defn swap!
  [scope f & args]
  (apply set-state! (:app-state scope) update-in (:path scope) f args))

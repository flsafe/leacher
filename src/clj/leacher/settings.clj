(ns leacher.settings
  (:require [com.stuartsierra.component :as component]
            [leacher.state :as state]))

(def template
  {:host               ""
   :port               119
   :ssl?               false
   :user               ""
   :password           ""
   :max-connections    10})

(defrecord Settings [app-state state]
  component/Lifecycle
  (start [this]
    (if-not state
      (let [state (state/new-scope app-state :settings)]
        (when (nil? @state)
          (state/reset! state template))
        (assoc this :state state))
      this))
  (stop [this]
    (if state
      (assoc this :state nil)
      this)))

(defn new-settings
  []
  (map->Settings {}))

;;

(defn reset-with!
  [settings new]
  (state/reset! (:state settings) new))

(defn get-setting
  [settings key]
  (get @(:state settings) key))

(defn all
  [settings]
  @(:state settings))

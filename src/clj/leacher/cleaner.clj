(ns leacher.cleaner
  (:require [clojure.core.async :as async :refer [<!! chan thread]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.state :as state]
            [leacher.workers :refer [worker]]
            [me.raynes.fs :as fs]))

(defn move-combined
  [cfg file]
  (let [combined (:combined file)
        complete (fs/file (-> cfg :dirs :complete)
                   (fs/base-name combined))]
    (log/debug "moving" (str combined) "to" (str complete))
    (fs/rename combined complete)))

(defn clean-segments
  [file]
  (doseq [segment (-> file :segments vals)
          :let [f (:downloaded segment)]
          :when (and f (fs/exists? f))]
    (log/debug "cleaner deleting" (str f))
    (fs/delete f)))

(defn clean
  [cfg app-state {:keys [filename] :as file}]
  (try
    (state/update-file! app-state filename assoc
      :cleaning-started-at (System/currentTimeMillis)
      :status :cleaning)

    (clean-segments file)
    (move-combined cfg file)

    (state/update-file! app-state filename assoc
      :status :completed
      :cleaning-finished-at (System/currentTimeMillis))

    (catch Exception e
      (log/error e "failed cleaning")
      (state/update-file! app-state filename assoc
        :status :failed
        :error (.getMessage e)
        :finished-at (System/currentTimeMillis)))))

(defn start-listening
  [cfg app-state {:keys [in]}]
  (worker "cleaner" in
    (fn [filename]
      (let [file (state/get-file app-state filename)]
        (clean cfg app-state file)))))

;; component

(defrecord Cleaner [cfg app-state channels]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in (chan)}]
        (log/info "starting")
        (start-listening cfg app-state channels)
        (assoc this :channels channels))
      this))
  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels nil))
      this)))

(defn new-cleaner
  [cfg]
  (map->Cleaner {:cfg cfg}))

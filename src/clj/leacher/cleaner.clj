(ns leacher.cleaner
  (:require [me.raynes.fs :as fs]
            [clojure.core.async :as async :refer [thread <!! >!! chan]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.state :as state]))

(defn start-listening
  [cfg app-state {:keys [in]}]
  (thread
    (loop []
      (if-let [{:keys [filename] :as file} (<!! in)]
        (do
          (log/info "got file:" filename)
          (doseq [segment (-> file :segments vals)
                  :let [f (:downloaded-file segment)]]
            (when f
              (log/info "cleaner deleting" (str f))
              (fs/delete f)))
          (let [combined (:combined-file file)
                complete (fs/file (-> cfg :dirs :complete)
                           (fs/base-name combined))]
            (log/info "moving" (str combined) "to" (str complete))
            (fs/rename combined complete))
          (state/set-state! app-state update-in
            [:downloads filename :status] :completed))

        (log/info "exiting")))))

;; component

(defrecord Cleaner [cfg app-state channels]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in (chan)}]
        (start-listening cfg app-state channels)
        (assoc this :channels channels))
      this))
  (stop [this]
    (if channels
      (do
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels nil))
      this)))

(defn new-cleaner
  [cfg]
  (map->Cleaner {:cfg cfg}))
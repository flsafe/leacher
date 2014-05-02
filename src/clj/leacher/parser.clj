(ns leacher.parser
  (:require [clojure.core.async :as async :refer [<!! >!! alt!! chan
                                                  thread]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.state :as state]
            [leacher.nzb :as nzb]
            [leacher.workers :refer [worker]]))

;; component

(defn start-listening
  [{:keys [in out]} app-state]
  (worker "parser" in
    (fn [f]
      (let [files (nzb/parse f)]
        (doseq [[filename file] files
                :when (not (state/get-file app-state filename))]
          (state/set-file! app-state filename
            (assoc file :status :waiting)))
        (doseq [[filename _] files]
          (log/info "putting" filename "on out chan")
          (>!! out filename))
        (fs/delete f)))))

(defrecord Parser [channels app-state]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in  (chan)
                      :out (chan)}]
        (start-listening channels app-state)
        (assoc this :channels channels))
      this))

  (stop [this]
    (if channels
      (do
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels nil))
      this)))

(defn new-parser
  []
  (map->Parser {}))

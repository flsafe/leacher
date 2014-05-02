(ns leacher.parser
  (:require [clojure.core.async :as async :refer [<!! >!! alt!! chan
                                                  thread]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.state :as state]
            [leacher.nzb :as nzb]))

;; component

(defn start-listening
  [{:keys [in out]} app-state]
  (thread
    (loop []
      (if-let [f (<!! in)]
        (do
          (try
            (let [files (nzb/parse f)]
              (doseq [[filename file] files
                      :when (not (state/get-file app-state filename))]
                (state/set-file! app-state filename
                  (assoc file :status :waiting)))
              (doseq [[filename _] files]
                (log/info "putting" filename "on out chan")
                (>!! out filename))
              (fs/delete f))
            (catch Exception e
              (log/error e "failed to parser nzb file:" (str f))))
          (recur))

        (log/info "exiting")))))

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

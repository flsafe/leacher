(ns leacher.downloader
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [>!! <!! thread chan put! close!]]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.nntp :as nntp]
            [leacher.state :as state]
            [leacher.settings :as settings]
            [leacher.workers :as w]
            [leacher.utils :refer [loge]])
  (:import [java.net Socket]))

;; component

(defn download-to-file
  [cfg conn n {:keys [file segment] :as val}]
  (let [filename    (:filename file)
        message-id  (:message-id segment)
        result-file (fs/file (-> cfg :dirs :temp) message-id)]
    (if-not (fs/exists? result-file)
      (do (nntp/group conn (-> file :groups first))
          (let [resp (nntp/article conn message-id)]
            (log/debugf "worker[%d]: copying bytes to %s" n (str result-file))
            (io/copy (:bytes resp) result-file)))
      (log/infof "%s already exists" (str result-file)))
    (assoc val :downloaded-path (fs/absolute-path result-file))))

(defn with-reconnecting
  [settings body-fn]
  (let [retry (atom true)
        wait  (atom 1)]
    (while @retry
      (try
        (let [user     (settings/get-setting settings :user)
              password (settings/get-setting settings :password)
              conn     (nntp/connect (settings/all settings))]
          (with-open [sock ^Socket (:socket conn)]
            (nntp/authenticate conn user password)
            (body-fn conn)
            (reset! retry false)))
        (catch java.net.SocketException e
          (log/errorf e "connection error, retrying in %ds" @wait)
          (Thread/sleep (* 1000 @wait))
          (swap! wait #(min (* % 2) 30)))))))

(defn start-worker
  [n cfg settings downloads]
  (thread
    (with-reconnecting settings
      (fn [conn]
        (loop []
          (when-let [{:keys [segment reply] :as work} (<!! downloads)]
            (>!! reply
              (try
                (download-to-file cfg conn n work)
                (catch Exception e
                  (log/error e "failed downloading" (:message-id segment))
                  (assoc val :error e))))
            (recur)))))))

(defn start-workers
  [cfg settings {:keys [downloads]}]
  (map #(start-worker % cfg settings downloads)
    (settings/get-setting settings :max-connections)))

(defrecord Downloader [cfg channels settings workers]
  component/Lifecycle
  (start [this]
    (if workers
      this
      (assoc this :workers (start-workers cfg settings channels))))
  (stop [this]
    (if workers
      (assoc this :workers nil)
      this)))

(defn new-downloader
  [cfg channels]
  (map->Downloader {:cfg      cfg
                    :channels channels}))

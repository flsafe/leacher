(ns leacher.downloader
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [>!! <!! thread chan put! close! alt!!]]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.nntp :as nntp]
            [leacher.config :as config]
            [leacher.settings :as settings]
            [leacher.utils :refer [loge]])
  (:import [java.net Socket]))

;; component

(defn download-to-file
  [conn n {:keys [file segment] :as work}]
  (let [filename    (:filename file)
        message-id  (:message-id segment)
        result-file (fs/file config/tmp-dir message-id)]
    (if-not (fs/exists? result-file)
      (do (nntp/group conn (-> file :groups first))
          (let [resp (nntp/article conn message-id)]
            (log/debugf "worker[%d]: copying bytes to %s" n (str result-file))
            (io/copy (:bytes resp) result-file)))
      (log/infof "%s already exists" (str result-file)))
    (fs/absolute-path result-file)))

(defn with-reconnecting
  [n settings body-fn]
  (let [retry (atom true)
        wait  (atom 1)]
    (while @retry
      (try
        (let [nntp-settings (settings/all settings)
              _             (log/infof "worker[%d] connecting with %s" n nntp-settings)
              conn          (nntp/connect nntp-settings)]
          (log/infof "worker[%d] connected OK" n)
          (with-open [sock ^Socket (:socket conn)]
            (nntp/authenticate conn nntp-settings)
            (body-fn conn)
            (reset! retry false)))
        (catch java.net.SocketException e
          (log/errorf e "connection error, retrying in %ds" @wait)
          (Thread/sleep (* 1000 @wait))
          (swap! wait #(min (* % 2) 30)))))))

(defn start-worker
  [settings {:keys [downloads shutdown]} n]
  (thread
    (with-reconnecting n settings
      (fn [conn]
        (loop []
          (log/infof "worker[%d] waiting" n)
          (alt!!
            downloads
            ([{:keys [segment reply] :as work}]
               (when work
                 (log/infof "worker[%d]: got segment %s" n (:message-id segment))
                 (>!! reply
                   (try
                     (assoc work :downloaded-path (download-to-file conn n work))
                     (catch Exception e
                       (log/error e "failed downloading" (:message-id segment))
                       (assoc work :error e))))
                 (recur)))
            shutdown
            ([_]
               (log/info "shutting down worker" n))))))))

(defn start-workers
  [settings channels]
  (doall
    (map (partial start-worker settings channels)
      (range (settings/get-setting settings :max-connections)))))

(defrecord Downloader [channels settings workers]
  component/Lifecycle
  (start [this]
    (if-not workers
      (do (log/info "starting")
          (assoc this :workers (start-workers settings channels)))
      this))
  (stop [this]
    (if workers
      (do (log/info "stopping")
          (assoc this :workers nil))
      this)))

(defn new-downloader
  []
  (map->Downloader {}))

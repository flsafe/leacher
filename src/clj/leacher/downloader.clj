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

(defn article-or-missing
  [conn message-id]
  (try
    (nntp/article conn message-id)
    (catch clojure.lang.ExceptionInfo e
      (if (= 430 (-> e ex-data :code))
        :missing
        (throw e)))))

(defn try-groups
  [conn n result-file file segment]
  (loop [groups (:groups file)]
    (when groups
      (let [group      (first groups)
            message-id (:message-id segment)]
        (nntp/group conn (-> file :groups first))
        (let [resp (article-or-missing conn message-id)]
          (if (= :missing resp)
            (do (log/infof "worker[%d]: failed to dl %s from %s, trying next group"
                  n message-id group)
                (recur (next groups)))
            (io/copy (:bytes resp) result-file)))))))

(defn download-to-file
  [conn n {:keys [file segment] :as work}]
  (let [message-id  (:message-id segment)
        result-file (fs/file config/tmp-dir message-id)]
    (if-not (fs/exists? result-file)
      (try-groups conn n result-file file segment)
      (log/infof "worker[%d]: %s already exists" n (str result-file)))
    (fs/absolute-path result-file)))

(defn with-reconnecting
  [n settings body-fn]
  (let [retry (atom true)
        wait  (atom 1)]
    (while @retry
      (try
        (let [nntp-settings (settings/all settings)
              conn          (nntp/connect nntp-settings)]
          (with-open [sock ^Socket (:socket conn)]
            (nntp/authenticate conn nntp-settings)
            (log/infof "worker[%d]: authenticated" n)
            (try (body-fn conn)
                 (catch Exception e
                   (log/errorf e "worker[%d]: unexpected error, worker dead!" n)))
            (reset! retry false)))
        (catch java.net.SocketException e
          (log/errorf e "worker[%d]: connection error, retrying in %ds" n @wait)
          (Thread/sleep (* 1000 @wait))
          (swap! wait #(min (* % 2) 30)))))))

(defn start-worker
  [settings {:keys [downloads shutdown]} n]
  (thread
    (log/infof "worker[%d]: starting" n)
    (with-reconnecting n settings
      (fn [conn]
        (loop []
          (log/debugf "worker[%d] waiting" n)
          (alt!!
            shutdown
            ([_]
               (log/info "shutting down worker" n))

            downloads
            ([{:keys [segment reply] :as work}]
               (when work
                 (log/debugf "worker[%d]: got segment %s" n (:message-id segment))
                 (>!! reply
                   (try
                     (log/debugf "worker[%d]: downloading %s" n (:message-id segment))
                     (let [path (download-to-file conn n work)]
                       (log/debugf "worker[%d]: finished %s" n (:message-id segment))
                       (assoc work :downloaded-path path))
                     (catch Exception e
                       (log/errorf e "worker[%d]: failed downloading %s" n (:message-id segment))
                       (assoc work :error e))))
                 (recur)))

            :priority true))))))

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

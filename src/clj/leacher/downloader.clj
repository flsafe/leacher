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
    (if groups
      (let [group      (first groups)
            message-id (:message-id segment)]
        (nntp/group conn (-> file :groups first))
        (let [resp (article-or-missing conn message-id)]
          (if (= :missing resp)
            (do (log/infof "worker[%d]: %s not available in group %s, trying next group"
                  n message-id group)
                (recur (next groups)))
            (do (log/infof "worker[%d]: saving %s (%d bytes) to %s"
                  n message-id (count (:bytes resp)) result-file)
                (io/copy (:bytes resp) result-file)))))
      (throw (ex-info "failed to download segment" segment)))))

(defn download-to-file
  [conn n {:keys [file segment] :as work}]
  (let [message-id  (:message-id segment)
        result-file (fs/file config/tmp-dir message-id)]
    (if-not (fs/exists? result-file)
      (try-groups conn n result-file file segment)
      (log/infof "worker[%d]: %s already exists" n (str result-file)))
    (fs/absolute-path result-file)))

(defn connect-to-nntp
  [settings n]
  (log/infof "worker[%d]: connecting to nntp server" n)
  (let [nntp-settings (settings/all settings)
        conn          (nntp/new-connection nntp-settings)]
    (nntp/connect conn)
    (nntp/authenticate conn)
    (log/infof "worker[%d]: connected successfully" n)
    conn))

(defn start-worker
  [settings {:keys [downloads shutdown]} n]
  (thread
    (log/infof "worker[%d]: starting" n)
    (loop [conn (connect-to-nntp settings n)]
      (log/debugf "worker[%d] waiting" n)
      (alt!!
        shutdown
        ([_]
           (log/info "shutting down worker" n))

        downloads
        ([{:keys [file segment reply events] :as work}]
           (when work
             (log/debugf "worker[%d]: got segment %s" n (:message-id segment))
             (>!! reply
               (try
                 (log/debugf "worker[%d]: downloading %s" n (:message-id segment))
                 (let [path (download-to-file conn n work)]
                   (log/debugf "worker[%d]: finished %s" n (:message-id segment))
                   (put! events {:type     :segment-download-complete
                                 :filename (:filename file)})
                   (assoc work :downloaded-path path))
                 (catch Exception e
                   (log/errorf e "worker[%d]: failed downloading %s" n (:message-id segment))
                   (put! events {:type     :segment-download-failed
                                 :filename (:filename file)
                                 :message  (.getMessage e)})
                   (assoc work :error e))))
             (recur conn)))

        :priority true))))

(defn start-workers
  [settings channels]
  (doall
    (map (partial start-worker settings channels)
      (range (settings/get-setting settings :max-connections)))))

;;

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

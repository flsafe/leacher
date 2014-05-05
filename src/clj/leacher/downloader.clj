(ns leacher.downloader
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [>!! <!! thread chan put! close!]]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.nntp :as nntp]
            [leacher.state :as state]
            [leacher.workers :refer [workers]]
            [leacher.utils :refer [loge]])
  (:import [java.net Socket]))

;; component

(defn download-to-file
  [cfg app-state conn n file segment]
  (let [filename   (:filename file)
        message-id (:message-id segment)]
    (state/update-segment! app-state filename message-id
      assoc :status :downloading)

    (nntp/group conn (-> file :groups first))
    (let [resp   (nntp/article conn message-id)
          result (fs/file (-> cfg :dirs :temp) message-id)]
      (log/debugf "worker[%d]: copying bytes to %s" n (str result))
      (io/copy (:bytes resp) result)

      (state/update-file! app-state filename
        (fn [f]
          (-> f
            (update-in [:bytes-received] (fnil + 0) (:bytes segment))
            (update-in [:segments-completed] (fnil inc 0))
            (update-in [:segments message-id] assoc
              :status :downloaded
              :downloaded (fs/absolute-path result))))))))

(defn with-reconnecting
  [nntp-cfg app-state n body-fn]
  (let [retry (atom true)
        wait  (atom 1)]
    (while @retry
      (try
        (state/set-worker! app-state n :status :connecting)
        (let [{:keys [user password]} nntp-cfg
              conn                    (nntp/connect nntp-cfg)]
          (with-open [sock ^Socket (:socket conn)]
            (nntp/authenticate conn user password)
            (body-fn conn)
            (reset! retry false)))
        (catch java.net.SocketException e
          (state/set-worker! app-state n :status :error :message (.getMessage e))
          (log/errorf e "connection error, retrying in %ds" @wait)
          (Thread/sleep (* 1000 @wait))
          (swap! wait #(min (* % 2) 30)))))))

(defn start-workers
  [cfg app-state {:keys [work]}]
  (dotimes [n (-> cfg :nntp :max-connections)]
    (state/set-worker! app-state n :status :waiting))
  (workers (-> cfg :nntp :max-connections) "worker" work
    (fn [n {:keys [reply filename message-id] :as val}]
      (let [file    (state/get-file app-state filename)
            segment (get (file :segments) message-id)]
        (if (:cancelled segment)
          (do
            (log/info message-id "cancelled, skipping")
            (>!! reply :cancelled))
          (try
            (with-reconnecting (:nntp cfg) app-state n
              (fn [conn]
                (state/set-worker! app-state n :status :downloading)
                (download-to-file cfg app-state conn n file segment)
                (>!! reply :downloaded)
                (state/set-worker! app-state n :status :waiting)))
            (catch Exception e
              (log/error e "failed downloading" message-id)
              (state/update-segment! app-state filename message-id assoc
                :status :failed
                :error  (.getMessage e))
              (>!! reply :failed))))))))

(defn download
  [cfg file work]
  (let [replies  (chan)
        segments (filter #(not= :completed (:status %))
                   (vals (:segments file)))]

    (doseq [{:keys [message-id]} segments]
      (put! work
        {:reply      replies
         :filename   (:filename file)
         :message-id message-id}))

    (let [results (<!! (async/reduce conj []
                         (async/take (count segments) replies)))]
      (cond
        (every? #(= :downloaded %) results) :downloaded
        (some #{:cancelled} results)       :cancelled
        (some #{:failed} results)          :failed))))

(defn start-download
  [cfg app-state {:keys [work out]} {:keys [filename status] :as file}]
  (try
    (state/update-file! app-state filename assoc
      :downloading-started-at (System/currentTimeMillis)
      :status :downloading)

    (let [result (download cfg file work)]
      (state/update-file! app-state filename assoc
        :downloading-finished-at (System/currentTimeMillis)
        :status result)

      (when (= :downloaded result)
        (>!! out filename)))
    (catch Exception e
      (state/update-file! app-state filename assoc
        :status :failed
        :error (.getMessage e))
      (log/error e "failed downloading"))))

(defn resume-incomplete
  [cfg app-state {:keys [in]}]
  (loge "restarting incomplete"
    (when-let [files (state/get-downloads app-state)]
      (doseq [[filename file] files
              :when (and (not= :completed (:status file))
                      (not (:cancelled file)))]
        (log/info "resuming" filename)
        (put! in filename)))))

(defn start-listeners
  [cfg app-state {:keys [in] :as channels}]
  (workers (-> cfg :nntp :max-file-downloads) "listener" in
    (fn [n filename]
      (let [file (state/get-file app-state filename)]
        (when-not (:cancelled file)
          (start-download cfg app-state channels file)))))
  (resume-incomplete cfg app-state channels))

(defrecord Downloader [cfg app-state channels]
  component/Lifecycle
  (start [this]
    (if channels
      this
      (let [channels {:in     (chan 10)
                      :out    (chan 10)
                      :cancel (chan)
                      :work   (chan)}]
        (log/info "starting")
        (start-listeners cfg app-state channels)
        (start-workers cfg app-state channels)
        (assoc this :channels channels))))
  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [[_ ch] channels]
          (close! ch))
        (assoc this :channels nil))
      this)))

(defn new-downloader
  [cfg]
  (map->Downloader {:cfg cfg}))

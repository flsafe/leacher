(ns leacher.downloader
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [>!! <!! thread chan put! close!]]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.nntp :as nntp]
            [leacher.state :as state]
            [leacher.workers :as w]
            [leacher.utils :refer [loge]])
  (:import [java.net Socket]))

;; component

(defn download-to-file
  [cfg conn n file segment]
  (let [filename   (:filename @file)
        message-id (:message-id @segment)]
    (state/assoc! segment :status :downloading)

    (nntp/group conn (-> @file :groups first))
    (let [resp   (nntp/article conn message-id)
          result (fs/file (-> cfg :dirs :temp) message-id)]
      (log/debugf "worker[%d]: copying bytes to %s" n (str result))
      (io/copy (:bytes resp) result)

      (state/update-in! file [:bytes-received]
        (fnil + 0) (:bytes @segment))
      (state/update-in! file [:segments-completed]
        (fnil inc 0))
      (state/assoc! segment
        :status :downloaded
        :downloaded (fs/absolute-path result)))))

(defn with-reconnecting
  [nntp-cfg worker body-fn]
  (let [retry (atom true)
        wait  (atom 1)]
    (while @retry
      (try
        (state/assoc! worker :status :connecting)
        (let [{:keys [user password]} nntp-cfg
              conn                    (nntp/connect nntp-cfg)]
          (with-open [sock ^Socket (:socket conn)]
            (nntp/authenticate conn user password)
            (body-fn conn)
            (reset! retry false)))
        (catch java.net.SocketException e
          (log/errorf e "connection error, retrying in %ds" @wait)
          (state/assoc! worker :status :error :message (.getMessage e))
          (Thread/sleep (* 1000 @wait))
          (swap! wait #(min (* % 2) 30)))))))

(defn start-workers
  [cfg workers {:keys [work]}]
  (w/workers (-> cfg :nntp :max-connections) "dl-worker" work
    (fn [n {:keys [reply file segment] :as val}]
      (let [worker     (nth workers n)
            message-id (:message-id @segment)]
        (if (:cancelled @segment)
          (do
            (log/info message-id "cancelled, skipping")
            (>!! reply :cancelled))
          (try
            (with-reconnecting (:nntp cfg) worker
              (fn [conn]
                (state/assoc! worker :status :downloading)
                (download-to-file cfg conn n file segment)
                (>!! reply :downloaded)
                (state/assoc! worker :status :waiting)))
            (catch Exception e
              (log/error e "failed downloading" message-id)
              (state/assoc! segment
                :status :failed
                :error  (.getMessage e))
              (>!! reply :failed))))))))

(defn download
  [cfg file work]
  (let [message-ids (mapv :message-id
                      (filter #(not= :completed (:status %))
                        (-> @file :segments vals)))
        number      (count message-ids)
        replies     (chan number)]

    (doseq [message-id message-ids
            :let [segment (state/new-scope file :segments message-id)]]
      (put! work
        {:reply   replies
         :file    file
         :segment segment}))

    (let [results (<!! (async/reduce conj []
                         (async/take number replies)))]
      (cond
        (every? #(= :downloaded %) results) :downloaded
        (some #{:cancelled} results)       :cancelled
        (some #{:failed} results)          :failed))))

(defn start-download
  [cfg {:keys [work out]} file]
  (try
    (state/assoc! file
      :downloading-started-at (System/currentTimeMillis)
      :status :downloading)

    (let [result (download cfg file work)]
      (state/assoc! file
        :downloading-finished-at (System/currentTimeMillis)
        :status result)

      (when (= :downloaded result)
        (>!! out file)))
    (catch Exception e
      (log/error e "failed downloading")
      (state/assoc! file
        :status :failed
        :error (.getMessage e)))))

(defn resume-incomplete
  [cfg downloads {:keys [in]}]
  (loge "restarting incomplete"
    (doseq [[filename src-file] @downloads
            :when (and (not= :completed (:status src-file))
                    (not (:cancelled src-file)))
            :let [file (state/new-scope downloads filename)]]
      (log/info "resuming" filename)
      (put! in file))))

(defn start-listeners
  [cfg {:keys [in] :as channels}]
  (w/workers (-> cfg :nntp :max-file-downloads) "dl-listener" in
    (fn [n file]
      (when-not (:cancelled @file)
        (start-download cfg channels file)))))

(defn build-worker-state
  [cfg app-state]
  (let [workers (mapv #(state/new-scope app-state :workers %)
                  (range (-> cfg :nntp :max-connections)))]
    (doseq [worker workers]
      (state/assoc! worker :status :waiting))
    workers))

(defrecord Downloader [cfg app-state channels]
  component/Lifecycle
  (start [this]
    (if channels
      this
      (let [channels  {:in     (chan 10)
                       :out    (chan 10)
                       :cancel (chan)
                       :work   (chan)}
            workers   (build-worker-state cfg app-state)
            downloads (state/new-scope app-state :downloads)]
        (log/info "starting")
        (start-listeners cfg channels)
        (resume-incomplete cfg downloads channels)
        (start-workers cfg workers channels)
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

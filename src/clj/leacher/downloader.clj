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
  [cfg conn n file segment]
  (let [filename    (:filename @file)
        message-id  (:message-id @segment)
        result-file (fs/file (-> cfg :dirs :temp) message-id)]

    (if-not (fs/exists? result-file)
      (do (state/assoc! segment :status :downloading)
          (nntp/group conn (-> @file :groups first))
          (let [resp (nntp/article conn message-id)]
            (log/debugf "worker[%d]: copying bytes to %s" n (str result-file))
            (io/copy (:bytes resp) result-file)))
      (log/infof "%s already exists, not downloading again" (str result-file)))

    (state/update-in! file [:bytes-received]
      (fnil + 0) (:bytes @segment))
    (state/update-in! file [:segments-completed]
      (fnil inc 0))
    (state/assoc! segment
      :status :downloaded
      :downloaded (fs/absolute-path result-file))))

(defn with-reconnecting
  [settings worker body-fn]
  (let [retry (atom true)
        wait  (atom 1)]
    (while @retry
      (try
        (state/assoc! worker :status :connecting)
        (let [user     (settings/get-setting settings :user)
              password (settings/get-setting settings :password)
              conn     (nntp/connect (settings/all settings))]
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
  [cfg settings workers {:keys [work]}]
  (dotimes [n (settings/get-setting settings :max-connections)]
    (let [worker (nth workers n)]
      (thread
        (with-reconnecting settings worker
          (fn [conn]
            (loop []
              (state/assoc! worker :status :waiting)
              (when-let [{:keys [reply file segment] :as val} (<!! work)]
                (if (:cancelled @segment)
                  (do (log/info (:message-id @segment) "cancelled, skipping")
                      (>!! reply :cancelled))
                  (do (try
                        (state/assoc! worker :status :downloading)
                        (download-to-file cfg conn n file segment)
                        (>!! reply :downloaded)
                        (catch Exception e
                          (log/error e "failed downloading" (:message-id @segment))
                          (state/assoc! segment
                            :status :failed
                            :error  (.getMessage e))
                          (>!! reply :failed)))))
                (recur)))))))))

(defn download
  [file work]
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
  [{:keys [work out]} file]
  (try
    (state/assoc! file
      :downloading-started-at (System/currentTimeMillis)
      :status :downloading)

    (let [result (download file work)]
      (state/assoc! file
        :downloading-finished-at (System/currentTimeMillis)
        :status result)

      (when (not= :cancelled result)
        (>!! out file)))
    (catch Exception e
      (log/error e "failed downloading")
      (state/assoc! file
        :status :failed
        :error (.getMessage e)))))

(defn resume-incomplete
  [downloads {:keys [in]}]
  (loge "restarting incomplete"
    (doseq [[filename src-file] @downloads
            :when (and (not= :completed (:status src-file))
                    (not (:cancelled src-file)))
            :let [file (state/new-scope downloads filename)]]
      (log/info "resuming" filename)
      (put! in file))))

(defn start-listeners
  [cfg {:keys [in] :as channels}]
  (w/workers (:max-file-downloads cfg) "dl-listener" in
    (fn [n file]
      (let [result-path (fs/file (-> cfg :dirs :complete)
                          (:filename @file))
            exists?     (fs/exists? result-path)]
       (if-not (or (:cancelled @file) exists?)
         (start-download channels file)
         (do
           (when exists?
             (log/info "completed file exists at" (:filename @file))
             (state/assoc! file :status :completed))
           (log/info "skipping file" (:filename @file))))))))

(defn build-worker-state
  [settings app-state]
  (let [workers (mapv #(state/new-scope app-state :workers %)
                  (range (settings/get-setting settings :max-connections)))]
    (doseq [worker workers]
      (state/assoc! worker :status :waiting))
    workers))

(defrecord Downloader [cfg app-state channels settings]
  component/Lifecycle
  (start [this]
    (if channels
      this
      (let [channels  {:in   (chan 10)
                       :out  (chan 10)
                       :work (chan)}
            workers   (build-worker-state settings app-state)
            downloads (state/new-scope app-state :downloads)]
        (log/info "starting")
        (start-listeners cfg channels)
        (resume-incomplete downloads channels)
        (start-workers cfg settings workers channels)
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

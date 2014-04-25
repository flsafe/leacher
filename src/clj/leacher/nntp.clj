(ns leacher.nntp
  (:require [clojure.core.async :as async :refer [<!! >!! alt!! chan
                                                  close! put! thread]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.state :as state]
            [leacher.utils :refer [parse-long]]
            [me.raynes.fs :as fs])
  (:import (java.io BufferedReader ByteArrayOutputStream Writer)
           (java.net Socket)
           (javax.net.ssl SSLSocketFactory)))

(def ENCODING "ISO-8859-1")

(def DOT      (int \.))
(def RETURN   (int \return))
(def NEW_LINE (int \newline))

(declare response)

(defn connect
  [{:keys [host port ssl?]}]
  (let [socket (if ssl?
                 (.createSocket (SSLSocketFactory/getDefault)
                   ^String host ^int port)
                 (Socket. ^String host ^Long port))
        conn   {:socket socket
                :in     (io/reader socket :encoding ENCODING)
                :out    (io/writer socket :encoding ENCODING)}]
    (log/debug "initial response" (response conn))
    conn))

(defn write
  [conn ^String msg]
  (doto ^Writer (:out conn)
        (.write msg)
        (.write ^int RETURN)
        (.write ^int NEW_LINE)
        (.flush)))

(def response-re #"(\d{3}) (.*)")

(defn response
  [{^BufferedReader in :in}]
  (let [resp          (.readLine in)
        [_ code body] (re-find response-re resp)
        code          (parse-long code)
        resp          {:code code
                       :body body}]
    (if (>= code 400)
      (throw (ex-info "error response" resp))
      (log/spy resp))))

(defn header-response
  [{^BufferedReader in :in}]
  (loop [res {}]
    (let [line (.readLine in)]
      ;; headers are terminated by a blank line
      (if (string/blank? line)
        res
        (let [[k v] (string/split line #":" 2)
              k     (keyword (string/lower-case k))
              v     (string/trim v)]
          (recur (assoc res k v)))))))

(defn byte-body-response
  [{^BufferedReader in :in}]
  (let [b   (ByteArrayOutputStream.)]
    (loop [previous nil]
      (let [c (.read in)]
        ;; if first char after newline is dot, check for dot-stuffing
        (if (and (= NEW_LINE previous) (= DOT c))
          (let [c2 (.read in)]
            (condp = c2
              ;; undo dot-stuffing, throw away one dot
              DOT
              (do
                (.write b c2)
                (recur c2))

              ;; end of message indicator, read and throw away crlf and return
              RETURN
              (do
                (.read in)
                (.toByteArray b))

              (throw (Exception. (str "unexpected character after '.':" c2)))))
          (do
            (.write b c)
            (recur c)))))))

(defn authenticate
  [conn user password]
  (log/debug "authenticating")
  (write conn (str "AUTHINFO USER " user))
  (let [resp (response conn)]
    (when (<= 300 (:code resp) 399)
      (write conn (str "AUTHINFO PASS " password))
      (response conn))))

(defn ping
  [conn]
  (log/debug "pinging")
  (write conn "DATE")
  (response conn))

(defn group
  [conn name-of]
  (log/debug "changing group:" name-of)
  (write conn (str "GROUP " name-of))
  (let [resp              (response conn)
        [number low high] (string/split (:body resp) #" ")]
    (assoc resp
      :number (parse-long number)
      :low    (parse-long low)
      :high   (parse-long high)
      :group  name-of)))

(defn article
  [conn message-id]
  (log/debug "getting article:" message-id)
  (write conn (str "ARTICLE " message-id))
  (let [resp    (response conn)
        headers (header-response conn)
        bytes   (byte-body-response conn)]
    (assoc resp
      :headers headers
      :bytes   bytes
      :size    (count bytes))))

;; component

(defn download-to-file
  [cfg app-state conn n {:keys [file segment] :as val}]
  (let [filename   (:filename file)
        message-id (:message-id segment)]
    (try
      (state/set-worker! app-state n
        :status :downloading
        :message-id message-id
        :filename   filename)
      (state/update-segment! app-state filename message-id assoc
        :status :downloading)

      (group conn (-> file :groups first))
      (let [resp   (article conn message-id)
            result (fs/file (-> cfg :dirs :temp) message-id)]
        (log/debugf "worker[%d]: copying bytes to %s" n (str result))
        (io/copy (:bytes resp) result)

        (state/update-segment! app-state filename message-id assoc
          :status :completed)

        (state/update-file! app-state filename
          (fn [f]
            (-> f
              (update-in [:bytes-received] (fnil + 0) (:bytes segment))
              (update-in [:segments-completed] (fnil inc 0)))))

        (-> val
          (assoc-in [:segment :downloaded] result)
          (dissoc :reply)))

      (catch Exception e
        (state/update-segment! app-state filename message-id assoc
          :status :failed
          :error  (.getMessage e))
        (log/errorf e "worker[%d]: failed downloading %s" n message-id)))))

(defn try-connect
  [cfg app-state n]
  (try
    (connect (:nntp cfg))
    (catch Exception e
      (state/set-worker! app-state n
        :status :fatal
        :message (.getMessage e))
      (log/error e "failed to connect"))))

(defn start-worker
  [cfg app-state {:keys [work]} n]
  (log/debugf "worker[%d]: starting" n)
  (thread
    (loop []
      (state/set-worker! app-state n :status :waiting)
      (if-let [{:keys [reply] :as val} (<!! work)]
        (do
          (try
            (when-let [conn (try-connect cfg app-state n)]
              (with-open [sock ^Socket (:socket conn)]
                (authenticate conn (-> cfg :nntp :user) (-> cfg :nntp :password))
                (let [result (download-to-file cfg app-state conn n val)]
                  (log/debugf "worker[%d]: replying" n)
                  (>!! reply result))))
            (catch Exception e
              (log/errorf e "worker[%d]: failed" n)
              (state/set-worker! app-state n
                :status :fatal :message (.getMessage e)))))))
          (recur)))

(defn start-workers
  [cfg app-state channels]
  (dotimes [n (-> cfg :nntp :max-connections)]
    (start-worker cfg app-state channels n)))

(defn reload-completed
  [cfg file]
  (let [segments (reduce-kv (fn [res message-id {:keys [status] :as segment}]
                              (if (= :completed status)
                                (let [result (fs/file (-> cfg :dirs :temp) message-id)]
                                  (log/info "reloading" message-id)
                                  (assoc res message-id
                                         (assoc segment :downloaded result)))
                                (assoc res message-id segment)))
                   {} (:segments file))]
    (assoc file :segments segments)))

(defn download
  [cfg file work]
  (let [replies  (chan)
        file     (reload-completed cfg file)
        segments (filter #(not= :completed (:status %))
                   (vals (:segments file)))]

    ;; put all segments on work queue for concurrent downloading
    (doseq [segment segments]
      (put! work
        {:reply      replies
         :file       file
         :segment    segment}))

    ;; combine all results back onto original file->segment
    (async/reduce (fn [res {:keys [segment]}]
                    (let [{:keys [message-id]} segment]
                      (assoc-in res [:segments message-id] segment)))
      file (async/take (count segments) replies))))

(defn start-download
  [cfg app-state {:keys [work out]} {:keys [filename] :as file}]
  (try
    (state/update-file! app-state filename assoc
      :started-at (System/currentTimeMillis))
    (let [result-ch (download cfg file work)]
      (state/update-file! app-state filename assoc
        :status :downloading)
      (>!! out (<!! result-ch)))
    (catch Exception e
      (state/update-file! app-state filename assoc
        :status :failed
        :error (.getMessage  e))
      (log/error e "failed downloading"))))

(defn resume-incomplete
  [cfg app-state channels]
  (try
    (when-let [files (state/get-downloads app-state)]
      (doseq [[filename file] files
              :when (not= :completed (:status file))]
        (log/info "resuming" filename)
        ;; TODO: this will reset the started-at of file, speed will be
        ;; wrong. problem? meh
        (start-download cfg app-state channels file)))
    (catch Exception e
      (log/error e "failed restarting incomplete"))))

;; could start n listening to have more than one nzb file on the go at once?
(defn start-listening
  [cfg app-state {:keys [in out work] :as channels}]
  (thread
    (resume-incomplete cfg app-state channels)
    (loop []
      (log/debug "waiting for work")
      (if-let [{:keys [filename] :as file} (<!! in)]
        (do
          (start-download cfg app-state channels file)
          (recur))
        (log/debug "exiting")))))

(defrecord Nntp [cfg app-state channels]
  component/Lifecycle
  (start [this]
    (if channels
      this
      (let [channels {:in   (chan)
                      :out  (chan)
                      :work (chan)}]
        (log/info "starting")
        (start-listening cfg app-state channels)
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

(defn new-nntp
  [cfg]
  (map->Nntp {:cfg cfg}))

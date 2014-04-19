(ns leacher.nntp
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.async :as async
             :refer [chan thread <!! >!! close! put! alt!!]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.utils :refer [parse-long]]
            [leacher.state :as state]
            [leacher.decoders.yenc :as yenc])
  (:import (java.net Socket)
           (java.io PrintWriter InputStreamReader BufferedReader Reader Writer ByteArrayOutputStream RandomAccessFile)
           (java.lang StringBuilder)))

(def ENCODING "ISO-8859-1")

(def DOT      (int \.))
(def RETURN   (int \return))
(def NEW_LINE (int \newline))

(declare response)

(defn connect
  [{:keys [host port]}]
  (let [socket (Socket. ^String host ^Long port)
        conn   {:socket socket
                :in     (io/reader socket :encoding ENCODING)
                :out    (io/writer socket :encoding ENCODING)}]
    (log/info "initial response" (response conn))
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
      resp)))

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
  (log/info "authenticating user")
  (write conn (str "AUTHINFO USER " user))
  (let [resp (response conn)]
    (when (<= 300 (:code resp) 399)
      (log/info "authenticating password")
      (write conn (str "AUTHINFO PASS " password))
      (response conn))))

(defn group
  [conn name-of]
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
  (write conn (str "ARTICLE " message-id))
  (let [resp    (response conn)
        headers (header-response conn)
        bytes   (byte-body-response conn)]
    (assoc resp
      :headers headers
      :bytes   bytes
      :size    (count bytes))))

;; component

(defn authenticated?
  [conn {:keys [user password]}]
  (try
    (log/info (authenticate conn user password))
    true
    (catch Exception e
      (log/error e "failed to authenticate with nntp server" (ex-data e))
      false)))

(defn download-to-file
  [cfg app-state conn n {:keys [file segment]}]
  (let [filename   (:filename file)
        message-id (:message-id segment)]
    (try
      (state/set-state! app-state assoc-in
        [:workers n] {:status     :downloading
                      :message-id message-id
                      :filename   filename})
      (state/set-state! app-state assoc-in
        [:downloads filename :segments message-id :status] :downloading)

      (log/infof "worker[%d]: switching group to %s" n (-> file :groups first))
      (group conn (-> file :groups first))

      (log/infof "worker[%d]: downloading article %s" n message-id)
      (let [resp   (article conn message-id)
            result (fs/file (-> cfg :dirs :temp) message-id)]
        (log/infof "worker[%d]: saving to %s" n (str result))
        (io/copy (:bytes resp) result)

        (state/set-state! app-state assoc-in
          [:downloads filename :segments message-id :status]
          :completed)
        (state/set-state! app-state update-in
          [:downloads filename :bytes-received] (fnil + 0) (count (:bytes resp)))

        result)
      (catch Exception e
        (state/set-state! app-state assoc-in
          [:downloads filename :segments message-id]
          {:status :failed
           :error  (.getMessage e)})
        (log/errorf e "failed downloading %s" message-id)))))

(defn start-worker
  [cfg app-state {:keys [work]} n]
  (log/info "starting worker" n)
  (thread
    (let [conn (connect (:nntp cfg))]
      (with-open [socket ^Socket (:socket conn)]
        (if (authenticated? conn (:nntp cfg))
          (loop []
            (log/infof "worker[%d]: waiting for work" n)
            (state/set-state! app-state assoc-in
              [:workers n] {:status :waiting})
            (alt!!
              work
              ([val]
                 (if val
                   (do
                     (let [file (download-to-file cfg app-state conn n val)]
                       (log/infof "worker[%d]: replying" n)
                       (>!! (:reply val)
                         (-> val
                           (dissoc :reply)
                           (assoc-in [:segment :downloaded-file] file))))
                     (recur))
                   (log/infof "worker[%d] exiting" n)))))
          (state/set-state! app-state assoc-in
            [:workers n]
            {:status :error :message "Failed to authenticate"}))))
    (log/infof "worker[%d] socket closing" n)))

(defn start-workers
  [cfg app-state channels]
  (mapv #(start-worker cfg app-state channels %)
    (range (:max-connections cfg))))

(defn download
  [{:keys [filename] :as file} work]
  (let [replies  (chan)
        segments (:segments file)]
    (doseq [segment (vals segments)]
      (put! work
        {:reply      replies
         :file       file
         :segment    segment}))
    (async/reduce (fn [res {:keys [segment]}]
                    (let [{:keys [message-id]} segment]
                      (assoc-in res [:segments message-id] segment)))
      file (async/take (count segments) replies))))

;; could start n listening to have more than one nzb file on the go at once?
(defn start-listening
  [app-state {:keys [in out work]}]
  (thread
    (loop []
      (alt!!
        in
        ([{:keys [filename] :as file}]
           (if file
             (do
               (state/set-state! app-state assoc-in
                 [:downloads filename :status] :starting)
               (let [result-ch (download file work)]
                 (state/set-state! app-state assoc-in
                   [:downloads filename :status] :downloading)
                 (>!! out (<!! result-ch))
                 (recur)))
             (log/info "exiting loop")))))))

(defrecord Nntp [cfg app-state channels]
  component/Lifecycle
  (start [this]
    (if channels
      this
      (let [channels {:in   (chan)
                      :out  (chan)
                      :work (chan)}]
        (log/info "starting")
        (start-listening app-state channels)
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

;; public

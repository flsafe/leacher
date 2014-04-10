(ns leacher.nntp
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.async :as async
             :refer [chan thread <!! >!! close! put!]]
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
    (log/info "initial response" (pr-str (response conn)))
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
      (ex-info "error response" resp)
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
  (write conn (str "AUTHINFO USER " user))
  (let [resp (response conn)]
    (when (<= 300 (:code resp) 399)
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

(defn start-worker
  [cfg work-chan app-state n]
  (log/info "starting worker" n)
  (thread
    (let [conn (connect cfg)]
      (with-open [socket ^Socket (:socket conn)]
        (try
          (log/info "auth resp:" (authenticate conn (:user cfg) (:password cfg)))
          (catch Exception e
            (log/error e "failed to authenticate with nntp server" (ex-data e))
            (throw e)))
        (loop []
          (state/set-state! app-state assoc-in
                            [:nntp :workers n :status] :waiting)
          (if-let [work (<!! work-chan)]
            (do
              (log/info "worker" n "got work" work)
              (try
                (log/info (group conn (-> work :file :groups first)))
                (let [resp (article conn (-> work :segment :message-id))]
                  (log/info "putting resp on chan" resp)
                  (>!! (:reply work) (-> work
                                         (dissoc :reply)
                                         (assoc :resp resp))))
                (catch Exception e
                  (log/error e "failed to download segment")))
              (recur))
            (log/info "worker" n "exiting"))))
      (log/info "socket closing" (:socket conn)))))

(defn start-workers
  [cfg work-chan app-state]
  (mapv #(start-worker cfg work-chan app-state %)
        (range (:max-connections cfg))))

(defrecord Nntp [cfg workers work-chan app-state]
  component/Lifecycle
  (start [this]
    (log/info "starting")
    (if work-chan
      this
      (let [work-chan (chan)]
        (assoc this
          :workers (start-workers cfg work-chan app-state)
          :work-chan work-chan))))
  (stop [this]
    (log/info "stopping")
    (if workers
      (do
        (close! work-chan)
        (dissoc this :workers :work-chan))
      this)))

(defn new-nntp
  [cfg]
  (map->Nntp {:cfg cfg}))

;; public

(defn decode-segment
  [{:keys [output] :as file} segment]
  (let [^RandomAccessFile output output
        {:keys [keywords bytes]} (yenc/decode-segment (:file segment))
        begin (-> keywords :part :begin dec)
        end   (-> keywords :part :end dec)]
    (log/info "decoding" begin "->" end)
    (.seek output begin)
    (.write output ^bytes bytes)
    file))

(defn download-segment
  [dir {:keys [resp segment] :as res}]
  (let [f (io/file dir (:message-id segment))]
    (try
      (io/copy (:bytes resp) f)
      (assoc res :file f)
      (catch Exception e
        (log/error e "failed to write segment")
        (assoc res :error e)))))

(defn download
  [nntp file dir]
  (let [reply    (chan)
        segments (:segments file)
        amount   (count segments)]
    (doseq [segment segments]
      (put! (:work-chan nntp)
            {:reply   reply
             :file    file
             :segment segment}))
    (let [f (RandomAccessFile. (fs/file dir (:filename file)) "rw")]
      (->> (async/take amount reply)
           (async/map< #(download-segment dir %))
           (async/take amount)
           (async/reduce decode-segment (assoc file :output f))))))

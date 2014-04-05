(ns leacher.nntp
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.async :refer [chan thread <!! >!! close!]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.utils :refer [parse-long]]
            [leacher.state :as state])
  (:import (java.net Socket)
           (java.io PrintWriter InputStreamReader BufferedReader Reader Writer ByteArrayOutputStream)
           (java.lang StringBuilder)))

(def ENCODING "ISO-8859-1")

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
    (.write (int \return))
    (.write (int \newline))
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
      (if (string/blank? line)
        res
        (let [[k v] (string/split line #":" 2)
              k     (keyword (string/lower-case k))
              v     (string/trim v)]
              (recur (assoc res k v)))))))

(defn byte-body-response
  [{^BufferedReader in :in}]
  (let [b   (ByteArrayOutputStream.)
        dot (int \.)
        r   (int \return)
        lf  (int \newline)]
   (loop [previous nil]
     (let [c (.read in)]
       ;; if first char after newline is dot, check for dot-stuffing
       (if (and (= lf previous) (= dot c))
         (let [c2 (.read in)]
           (condp = c2
             ;; undo dot-stuffing, throw away one dot
             dot
             (do
               (.write b c2)
               (recur c))

             ;; end of message indicator, read and throw away crlf and return
             r
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
          (authenticate conn (:user cfg) (:password cfg))
          (catch Exception e
            (log/error e "failed to authenticate with nntp server" (ex-data e))
            (throw e)))
        (loop []
          (state/set-state! app-state assoc-in
                            [:nntp :workers n :status] :waiting)
          (if-let [work (<!! work-chan)]
            (do
              (log/info "worker" n "got work" work)
              ;; TODO - process the download. should work pass in chan
              ;; to receive reply on?
              (recur))
            (log/info "worker" n "exiting")))))))

(defn start-workers
  [cfg work-chan app-state]
  (mapv #(start-worker cfg work-chan app-state %)
    (range (:max-connections cfg))))

(defrecord Nntp [cfg workers work-chan app-state]
  component/Lifecycle
  (start [this]
    (log/info "starting")
    (if workers
      this
      (assoc this
        :workers (start-workers cfg work-chan app-state)
        :work-chan work-chan)))
  (stop [this]
    (log/info "stopping")
    (if workers
      (do
        (close! work-chan)
        (dissoc this :workers :work-chan))
      this)))

(defn new-nntp
  [cfg work-chan]
  (map->Nntp {:cfg       cfg
              :work-chan work-chan}))

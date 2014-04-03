(ns leacher.nntp
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.async :refer [chan thread <!! >!! close!]]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.utils :refer [parse-long]])
  (:import (java.net Socket)
           (java.io PrintWriter InputStreamReader BufferedReader Reader Writer)))

(def ENCODING "ISO-8859-1")

(defn connect
  [{:keys [host port]}]
  (let [socket (Socket. ^String host ^Long port)]
    {:socket socket
     :in     (io/reader socket :encoding ENCODING)
     :out    (io/writer socket :encoding ENCODING)}))

(defn write
  [conn msg]
  (doto ^Writer (:out conn)
    (.write (str msg "\r\n"))
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

(defn multiline-response
  ([conn]
     (multiline-response conn #"^\.$"))
  ([{^BufferedReader in :in} terminator-re]
     (loop [result []]
       (let [line (.readLine in)]
         (if (re-find terminator-re line)
           result
           (recur (conj result line)))))))

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

(defn parse-headers
  [lines]
  (reduce (fn [m l]
            (let [[k v] (string/split l #":" 2)]
              (assoc m (keyword (string/lower-case k)) (string/trim v))))
          {} lines))

(defn parse-body
  [lines]
  (.getBytes ^String (string/join "\r\n" lines) ^String ENCODING))

(defn article
  [conn message-id]
  (write conn (str "ARTICLE " message-id))
  (let [resp    (response conn)
        headers (parse-headers (multiline-response conn #"^ *$"))
        bytes   (parse-body (multiline-response conn))]
    (assoc resp
      :headers headers
      :bytes   bytes
      :size    (count bytes))))

;; component

(defn start-worker
  [config work-chan n]
  (log/info "starting worker" n)
  (thread
    (let [conn (connect config)]
      (with-open [socket ^Socket (:socket conn)]
        (loop []
          (if-let [work (<!! work-chan)]
            (do
              (log/info "worker" n "got work" work)
              (recur))
            (log/info "worker" n "exiting")))))))

(defn start-workers
  [config work-chan]
  (mapv #(start-worker config work-chan %)
    (range (:max-connections config))))

(defrecord Nntp [config workers work-chan]
  component/Lifecycle
  (start [this]
    (log/info "starting")
    (if workers
      this
      (assoc this
        :workers (start-workers config work-chan)
        :work-chan work-chan)))
  (stop [this]
    (log/info "stopping")
    (if workers
      (do
        (close! work-chan)
        (dissoc this :workers :work-chan))
      this)))

(defn new-nntp
  [config work-chan]
  (map->Nntp {:config    config
              :work-chan work-chan}))

(comment



  )

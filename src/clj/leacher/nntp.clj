(ns leacher.nntp
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [leacher.utils :refer [parse-long]])
  (:import (java.io BufferedReader ByteArrayOutputStream Writer)
           (java.net Socket)
           (javax.net.ssl SSLSocketFactory)))

(def ENCODING "ISO-8859-1")

(def DOT      (int \.))
(def RETURN   (int \return))
(def NEW_LINE (int \newline))

(declare response)

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

(defprotocol NntpConnection
  (connect [this])
  (re-connect [this])
  (with-reconnect-if-timeout [this body-fn])
  (close [this])
  (authenticate [this])
  (group [this name-of])
  (article [this message-id]))

(defn new-connection
  [opts]
  (let [conn          (atom nil)
        current-group (atom nil)]
    (reify NntpConnection
      
      (connect [this]
        (let [{:keys [host port ssl?]} opts
              socket (if ssl?
                       (.createSocket (SSLSocketFactory/getDefault)
                         ^String host ^int port)
                       (Socket. ^String host ^Long port))]
          (reset! conn {:socket socket
                        :in     (io/reader socket :encoding ENCODING)
                        :out    (io/writer socket :encoding ENCODING)})
          (response @conn)))

      (re-connect [this]
        (connect this)
        (when @current-group
          (group this @current-group)))

      (with-reconnect-if-timeout [this body-fn]
        (try
          (body-fn)
          (catch clojure.lang.ExceptionInfo e
            (if (= 400 (-> e ex-data :code))
              (do (println "idle timeout, reconnecting")
                  (re-connect this)
                  (body-fn))
              (throw e)))))

      (authenticate [this]
        (with-reconnect-if-timeout this
          (fn []
            (let [{:keys [user password]} opts]
              (when (and user password)
                (write @conn (str "AUTHINFO USER " user))
                (let [resp (response @conn)]
                  (when (<= 300 (:code resp) 399)
                    (write @conn (str "AUTHINFO PASS " password))
                    (response @conn))))))))

      (group [this group-name]
        (reset! current-group group-name)
        (with-reconnect-if-timeout this
          (fn []
            (write @conn (str "GROUP " group-name))
            (let [resp              (response @conn)
                  [number low high] (string/split (:body resp) #" ")]
              (assoc resp
                :number (parse-long number)
                :low    (parse-long low)
                :high   (parse-long high)
                :group  group-name)))))

      (article [this message-id]
        (with-reconnect-if-timeout this
          (fn []
            (write @conn (str "ARTICLE " message-id))
            (let [resp    (response @conn)
                  headers (header-response @conn)
                  bytes   (byte-body-response conn)]
              (assoc resp
                :headers headers
                :bytes   bytes
                :size    (count bytes))))))

      (close [this]
        (.close ^Socket (:socket @conn))))))



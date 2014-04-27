(ns leacher.decoders.yenc
  (:require [clojure.core.async :as async
             :refer [<!! >!! chan put! thread]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [leacher.state :as state]
            [leacher.utils :refer [parse-long]])
  (:import (java.io BufferedReader RandomAccessFile)))

(def ENCODING "ISO-8859-1")

(def keyword-fns
  {:size  parse-long
   :line  parse-long
   :part  parse-long
   :end   parse-long
   :begin parse-long})

(defn fail-eof
  [i]
  (when (= -1 i)
    (throw (Exception. "unexpected eof")))
  i)

;; perhaps this is a bit silly, could just readLine the reader and use a
;; regexp to get the keywords O_o
(defn read-keywords
  [^BufferedReader  r]
  ;; skip remainder of keyword (eg 'egin' for ybegin)
  (loop []
    (let [i (fail-eof (.read r))
          c (char i)]
      (when-not (= \space c)
        (recur))))
  ;; process keywords
  (loop [state :key
         res   {}
         k     ""
         v     ""]
    (let [i (fail-eof (.read r))
          c (char i)]
      (condp = state
        :done  res
        :key   (if (= \= c)
                 (recur :value res k v)
                 (recur :key res (str k c) v))
        :value (condp = c
                 \space
                 (let [k (keyword k)
                       f (get keyword-fns k identity)
                       v (f v)]
                   (recur :key (assoc res k v) nil nil))

                 \return
                 (let [k (keyword k)
                       f (get keyword-fns k identity)
                       v (f v)]
                   (recur :done (assoc res k v) nil nil))

                 (recur :value res k (str v c)))))))

(defn decode
  [seg-file]
  (with-open [^BufferedReader r (io/reader seg-file :encoding ENCODING)
              w (java.io.ByteArrayOutputStream.)]
    (loop [state    :reading
           keywords {}]
      (let [i (int (fail-eof (.read r)))
            c (char i)]
        (condp = state
          :reading
          (condp = c
            \return  (recur :reading keywords)
            \newline (recur :reading keywords)
            \=       (recur :escape keywords)
            (do (.write w (int (mod (- i 42) 256)))
                (recur :reading keywords)))

          :escape
          (condp = c
            \y (recur :keyword-line keywords)
            (do
              (.write w (int (mod (- i 64 42) 256)))
              (recur :reading keywords)))

          :keyword-line
          (let [values (read-keywords r)]
            (condp = c
              \b (recur :reading (assoc keywords :begin values))
              \e {:keywords (assoc keywords :end values)
                  :bytes    (.toByteArray w)}
              \p (recur :reading (assoc keywords :part values)))))))))

(defn write-to
  [{:keys [segments filename]} file]
  (let [output (RandomAccessFile. ^java.io.File file "rw")]
    (doseq [segment (vals segments)
            :let [decoded (:decoded segment)]
            :when decoded]
      (let [begin (-> decoded :keywords :part :begin dec)
            end   (-> decoded :keywords :part :end dec)]
        (.seek output begin)
        (.write output ^bytes (:bytes decoded))))))

(defn valid-segment?
  [{:keys [keywords bytes] :as segment}]
  (let [begin (:begin keywords)
        end   (:end keywords)]
    (= (:size begin) (:size end) (count bytes))))

;; component

(defn decode-segment
  [{:keys [segment reply] :as val} n]
  (try
    (log/debugf "worker[%d]: got %s" n (:message-id segment))
    (if-let [segment-file (:downloaded segment)]
      (let [decoded (decode segment-file)]
        (>!! reply (-> val
                     (dissoc :reply)
                     (assoc-in [:segment :decoded] decoded))))
      (do
        (log/warnf "worker[%d]: no downloaded, skipping" n)
        (>!! reply (dissoc val :reply))))
    (catch Exception e
      (log/errorf e "worker[%d]: failed decoding" n)
      ;; TODO: handle error in some way
      (>!! reply (dissoc val :reply)))))

(defn start-workers
  [{:keys [decoders]} {:keys [work]} app-state]
  (dotimes [n decoders]
    (thread
      (loop []
        (if-let [val (<!! work)]
          (do
            (decode-segment val n)
            (recur))
          (log/debugf "worker[%d]: exiting" n))))))

(defn decode-file
  [file work]
  (let [segments (:segments file)
        replies  (chan (count segments))]
    ;; put all segments onto work queue for decoding concurrently
    ;; across workers
    (doseq [segment (vals segments)]
      (put! work
            {:reply   replies
             :segment segment}))

    ;; combine the results back onto the original file->segments
    (async/reduce (fn [res {:keys [segment]}]
                    (assoc-in res [:segments (:message-id segment)] segment))
                  file (async/take (count segments) replies))))

(defn process-file
  [cfg app-state {:keys [out work]} {:keys [filename] :as file}]
  (try
    (log/debug "got file" filename)
    (state/update-file! app-state filename assoc
      :status :decoding
      :decoding-started-at (System/currentTimeMillis))

    (let [file     (<!! (decode-file file work))
          combined (io/file (-> cfg :dirs :temp) filename)]
      (io/make-parents combined)
      (write-to file combined)

      (state/update-file! app-state filename assoc
        :status :waiting
        :decoding-finished-at (System/currentTimeMillis))

      (>!! out
        (assoc file :combined combined)))
    (catch Exception e
      (log/error e "failed decoding" filename)
      (>!! out file))))

(defn start-listening
  [cfg {:keys [in] :as channels} app-state]
  (thread
    (loop []
      (if-let [file (<!! in)]
        (do
          (process-file cfg app-state channels file)
          (recur))
        (log/debug "exiting")))))

(defrecord YencDecoder [cfg channels app-state]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in   (chan)
                      :out  (chan)
                      :work (chan)}]
        (log/info "starting")
        (start-workers cfg channels app-state)
        (start-listening cfg channels app-state)
        (assoc this :channels channels))
      this))

  (stop [this]
    (if channels
      (do
        (log/info "stopping")
        (doseq [[_ ch] channels]
          (async/close! ch))
        (assoc this :channels channels))
      this)))

(defn new-decoder
  [cfg]
  (map->YencDecoder {:cfg cfg}))

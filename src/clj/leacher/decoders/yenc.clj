(ns leacher.decoders.yenc
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [thread <!! >!! alt!! chan put!]]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [leacher.utils :refer [parse-long]])
  (:import [java.io StringReader BufferedReader BufferedWriter RandomAccessFile]))

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

(defn decode-segment
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

(defn start-workers
  [workers {:keys [work]}]
  (let [ctls (mapv chan (range workers))]
    (dotimes [n workers]
      (thread
        (loop []
          (if-let [{:keys [segment reply] :as val} (<!! work)]
            (do
              (try
                (log/infof "worker[%d]: got segment %s" n (:message-id segment))
                (if-let [segment-file (:downloaded-file segment)]
                  (let [decoded (decode-segment segment-file)]
                    (log/infof "worker[%d]: putting decoded on reply chan" n)
                    (>!! reply (-> val
                                 (dissoc :reply)
                                 (assoc-in [:segment :decoded] decoded))))
                  (do
                    (log/warnf "worker[%d]: missing :downloaded-file, skipping decoding" n)
                    (>!! reply (dissoc val :reply))))
                (catch Exception e
                  (log/errorf e "worker[%d]: failed decoding" n)
                  ;; TODO: handle error in some way
                  (>!! reply (dissoc val :reply))))
              (recur))
            (log/infof "worker[%d]: exiting" n)))))))

(defn decode-file
  [file work]
  (let [segments (:segments file)
        replies  (chan (count segments))]
    (doseq [segment (vals segments)]
      (put! work
            {:reply   replies
             :segment segment}))
    (async/reduce (fn [res {:keys [segment]}]
                    (assoc-in res [:segments (:message-id segment)] segment))
                  file (async/take (count segments) replies))))

(defn start-listening
  [cfg {:keys [in work out]}]
  (thread
    (loop []
      (if-let [{:keys [filename] :as file} (<!! in)]
        (do
          (log/info "got file to decode" filename)
          (let [result (<!! (decode-file file work))]
            (log/info "combining decoded parts for" filename)
            (let [combined-file (io/file (-> cfg :dirs :temp) (:filename file))]
              (io/make-parents combined-file)
              (write-to file combined-file)
              (log/info "putting combined file on out chan")
              (>!! out
                (assoc file :combined-file combined-file)))))
        (log/info "exiting listening loop")))))

(defrecord YencDecoder [cfg channels]
  component/Lifecycle
  (start [this]
    (if-not channels
      (let [channels {:in   (chan)
                      :out  (chan)
                      :ctl  (chan)
                      :work (chan)}]
        (log/info "starting")
        ;; TODO configure number
        (start-workers 10 channels)
        (start-listening cfg channels)
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

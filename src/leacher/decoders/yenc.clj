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
  [workers work-ch ctl]
  (let [ctls (mapv chan (range workers))]
    (thread
      (<!! ctl)
      (dotimes [n workers]
        (async/close! (nth ctls n))))
    (dotimes [n workers]
      (thread
        (loop []
          (alt!!
            work-ch
            ([{:keys [segment reply] :as work}]
               (log/infof "worker[%d]: got segment %s" n (:message-id segment))
               (try
                 (if-let [segment-file (:downloaded-file segment)]
                   (let [decoded (decode-segment segment-file)]
                     (log/infof "worker[%d]: putting decoded on reply chan" n)
                     (>!! reply (-> work
                                    (dissoc :reply)
                                    (assoc-in [:segment :decoded] decoded))))
                   (do
                     (log/warnf "worker[%d]: missing :downloaded-file, skipping decoding" n)
                     (>!! reply (dissoc work :reply))))
                 (catch Exception e
                   (log/errorf e "worker[%d]: failed decoding" n)
                   ;; TODO: handle error in some way
                   (>!! reply (dissoc work :reply))))
               (recur))

            (nth ctls n)
            ([_]
               (log/infof "worker[%d]: exiting" n))))))))


(defrecord YencDecoder [cfg work-ch ctl]
  component/Lifecycle
  (start [this]
    (if-not work-ch
      (let [work-ch (chan 100)
            ctl     (chan)]
        ;; TODO configure number
        (start-workers 10 work-ch ctl)
        (assoc this :work-ch work-ch :ctl ctl))
      this))

  (stop [this]
    (if work-ch
      (do
        (async/close! ctl)
        (async/close! work-ch)
        (assoc this :work-ch nil :ctl ctl))
      this)))

(defn new-decoder
  [cfg]
  (map->YencDecoder {:cfg cfg}))

;; public

(defn decode-file
  [yenc-decoder file]
  (let [segments (:segments file)
        replies  (chan (count segments))]
    (doseq [segment (vals segments)]
      (put! (:work-ch yenc-decoder)
            {:reply   replies
             :segment segment}))
    (async/reduce (fn [res {:keys [segment]}]
                    (assoc-in res [:segments (:message-id segment)] segment))
                  file (async/take (count segments) replies))))

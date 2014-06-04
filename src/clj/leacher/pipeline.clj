(ns leacher.pipeline
  (:require [leacher.nzb :as nzb]
            [leacher.nntp :as nntp]
            [leacher.decoders.yenc :as yenc]
            [me.raynes.fs :as fs]
            [clojure.core.async :as async :refer [onto-chan chan <!!]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import [java.io RandomAccessFile]))

(defn cleanup
  [cfg]
  (fn [{:keys [nzb nzb-file]}]
    (log/info "cleaning up temp files for" (str nzb-file))
    (doseq [file    (:files nzb)
            segment (-> file :segments vals)
            :let [f (:downloaded segment)]
            :when (and f (fs/exists? f))]
      (fs/delete f))
    (doseq [file (:files nzb)
            :let [combined (:combined file)
                  complete (fs/file (-> cfg :dirs :complete)
                             (fs/base-name combined))]]
      (log/info "moving" (str combined) "to" (str complete))
      (fs/rename combined complete))))

(defn decode
  [handler cfg {:keys [decodes]}]
  (fn [{:keys [nzb nzb-file] :as m}]
    (let [reply (chan)]
      (log/info "decoding segments for" (str nzb-file))
      (doseq [[filename file] (sort (:files nzb))
              [message-id segment] (:segments file)]
        (onto-chan decodes {:reply   reply
                            :nzb     nzb
                            :file    file
                            :segment segment}
          false))
      (log/info "waiting for decoding of segments for" (str nzb-file))
      (let [replies (async/take (:total-segments nzb) reply)]
        (loop []
          (when-let [{:keys [filename segment decoded]} (<!! replies)]
            (let [to-file (io/file (-> cfg :dirs :temp) filename)
                  begin   (-> decoded :keywords :part :begin dec)
                  end     (-> decoded :keywords :part :end dec)]
              (with-open [output (RandomAccessFile. to-file "rw")]
                (.seek output begin)
                (.write output ^bytes (:bytes decoded))))
            (recur)))))
    (log/info "finished decoding" (str nzb-file))
    (handler m)))

(defn download
  [handler cfg {:keys [downloads]}]
  (fn [{:keys [nzb nzb-file] :as m}]
    (let [reply (chan)]
      (log/info "downloading segments for" (str nzb-file))
      (doseq [[filename file] (sort (:files nzb))
              [message-id segment] (:segments file)]
        (onto-chan downloads {:reply   reply
                              :nzb     nzb
                              :file    file
                              :segment segment}
          false))
      (log/info "waiting for downloads of segments for" (str nzb-file))
      (let [nzb (loop [nzb     nzb
                       replies (async/take (:total-segments nzb) reply)]
                  (if-let [{:keys [filename segment downloaded-path]} (<!! replies)]
                    (recur (assoc-in nzb
                             [:files filename :segments (:message-id segment) :downloaded-path]
                             downloaded-path) replies)
                    nzb))]
        (log/info "finished downloading" (str nzb-file))
        (handler (assoc m :nzb nzb))))))

(defn parse
  [handler cfg]
  (fn [{:keys [nzb-file] :as m}]
    (let [complete-dir (-> cfg :dirs :complete)
          nzb          (assoc (nzb/parse nzb-file) :complete-dir complete-dir)]
      (log/info "parsed" (str nzb-file))
      (handler (assoc m :nzb nzb)))))

(defn log-errors
  [handler]
  (fn [m]
    (try (handler m)
         (catch Exception e
           (log/error e "error in pipeline")))))

(defn build-pipeline
  [cfg channels]
  (-> (cleanup cfg)
    (decode cfg channels)
    (download cfg channels)
    (parse cfg)
    log-errors))

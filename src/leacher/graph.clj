(ns leacher.graph
  (:require [clojure.core.async :as async :refer [chan go <!]]
            [com.stuartsierra.dependency :as dep]
            [leacher.nntp :as nntp]))

(defn download-task
  [file]
  {:type  :download
   :state :new
   :file  file})

(defn decode-task
  [file]
  {:type  :decode
   :state :new
   :file  file})

(defn combine-task
  [file]
  {:type  :combine
   :state :new
   :file  file})

(defn file-tasks
  [nntp graph file]
  (let [download (download-task file)
        decode   (decode-task file)]
    (-> graph
        (dep/depend decode download))))

(defn available?
  [graph task]
  (and (= :new (:state task))
       (every? #(= :complete (:state %))
               (dep/immediate-dependencies graph task))))

(defn available-tasks
  [graph]
  (filterv #(available? graph %)
           (dep/topo-sort graph)))

;; download -> decode(yenc) -> combine
(defn ->dag
  [nntp {:keys [files]}]
  (reduce #(file-tasks nntp %1 %2)
          (dep/graph) files))

(comment

  (let [g (->dag nil {:files [{:filename "a"}
                              {:filename "b"}
                              {:filename "c"}]})]
    (dep/topo-sort g))
  )

;; foreach file
;;   download each segment into a byte array
;;   write the segment to a file with message-id
;;   create an output file (name of (:filename file)) and yenc decode each segment file, using start/end to put into file in correct location (use a RandomAccessFile)
;; will now have a bunch of par2 files to combine

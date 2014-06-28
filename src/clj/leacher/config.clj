(ns leacher.config
  (:require [me.raynes.fs :as fs]))

(defonce home-dir
  (fs/absolute-path (fs/file (System/getProperty "user.home") ".leacher")))

(defonce settings-file
  (fs/absolute-path (fs/file home-dir "settings")))

(defonce complete-dir
  (fs/absolute-path (fs/file home-dir "complete")))

(defonce queue-dir
  (fs/absolute-path (fs/file home-dir "queue")))

(defonce tmp-dir
  (fs/absolute-path (fs/file home-dir "tmp")))

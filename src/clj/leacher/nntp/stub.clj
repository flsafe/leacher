(ns leacher.nntp.stub
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs]))

(defn connect
  [conn]
  (log/info "connected")
  {:code 200})

(defn authenticate
  [conn]
  (log/info "authenticated")
  {:code 200})

(defn group
  [conn group-name]
  (let [f (fs/file (-> conn :opts :root-dir) group-name)]
    (when-not (fs/exists? f)
      (throw (Exception. (format "%s does not exist" (fs/absolute-path f))))))
  (reset! (:current-group conn) group-name)
  {:code 210})

(defn article
  [conn message-id]
  (let [root-dir (-> conn :opts :root-dir)
        group    (-> conn :current-group deref)
        f        (fs/file root-dir group message-id)]
    (when-not (fs/exists? f)
      (throw (Exception. (format "%s does not exist" (fs/absolute-path f)))))
    (let [out (java.io.ByteArrayOutputStream.)]
      (io/copy f out)
      {:code  220
       :bytes (.toByteArray out)})))




(ns leacher.nntp
  (:require [leacher.nntp.impl :as impl]
            [leacher.nntp.stub :as stub]))

(defprotocol INntpConnection
  (connect [this])
  (authenticate [this])
  (group [this group-name])
  (article [this message-id]))

;;

(defrecord NntpConnection [conn current-group opts]
  INntpConnection
  (connect [this]
    (impl/connect this))
  (authenticate [this]
    (impl/authenticate this))
  (group [this group-name]
    (impl/group this group-name))
  (article [this message-id]
    (impl/article this message-id)))

(defn new-nntp-connection
  [opts]
  (map->NntpConnection {:conn          (atom nil)
                        :current-group (atom nil)
                        :opts          opts}))

;;

(defrecord StubNntpConnection [opts current-group]
  INntpConnection
  (connect [this]
    (stub/connect this))
  (authenticate [this]
    (stub/authenticate this))
  (group [this group-name]
    (stub/group this group-name))
  (article [this message-id]
    (stub/article this message-id)))

(defn new-stub-connection
  [{:keys [root-dir] :as opts}]
  (map->StubNntpConnection {:opts          opts
                            :current-group (atom nil)}))

;;

(defn new-connection
  [opts]
  (if (= :stub (:type opts))
    (new-stub-connection opts)
    (new-nntp-connection opts)))

(comment

  (def c (new-connection {:type     :stub
                          :root-dir "test-resources/stub-data"}))

  (connect c)
  (authenticate c)
  (group c "alt.binaries.test")
  (article c "<cats.yenc>")


  )

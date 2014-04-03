(ns scratch
  (:require [user :refer [go stop start reset]]
            [clojure.core.async :refer [put!]]))

(comment
  (go)
  (stop)
  (reset)

  (let [c (-> user/system :nntp :work-chan)]
    (put! c {:do "this"}))
  )

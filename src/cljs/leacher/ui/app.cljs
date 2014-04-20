(ns leacher.ui.app
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [goog.events :as events]
            [cljs.core.async :refer [put! <! chan close!]]
            [om.core :as om :include-macros true]
            [om.dom :as dom :include-macros true]
            [secretary.core :as secretary]
            [clojure.string :as string]
            [cljs.reader :refer [read-string]]
            [goog.net.WebSocket]
            [goog.net.WebSocket.MessageEvent]
            [goog.net.WebSocket.EventType :as Events])
  (:import [goog History]
           [goog.history EventType]))

(enable-console-print!)

(defn ws-chan
  []
  (let [c   (chan)
        ws  (doto (goog.net.WebSocket.)
              (goog.events.listen Events/OPENED
                (fn [e] (put! c {:type :opened :event e})))
              (goog.events.listen Events/CLOSED
                (fn [e] (close! c)))
              (goog.events.listen Events/ERROR
                (fn [e] (put! c {:type :error :event e})))
              (goog.events.listen Events/MESSAGE
                (fn [e] (let [data (cljs.reader/read-string (.-message e))]
                         (put! c {:type :message :event e :data data})))))]
    (.open ws "ws://localhost:8091/ws")
    c))

(def app-state (atom {:downloads {}
                      :workers   {}}))

(defmulti handle-event :type)

(defmethod handle-event :default
  [{:keys [type]}]
  (println "ws" (name type)))

(defmethod handle-event :message
  [{:keys [data]}]
  (swap! app-state merge (:data data)))

(defn ->bytes-display
  [b]
  (when b
    (cond
      (< b 1024)     (str b "b")
      (< b 1048576)  (str (.toFixed (/ b 1024) 2) "kb")
      (< b 1.074e+9) (str (.toFixed (/ b 1024 1024) 2) "mb")
      (< b 1.1e+12)  (str (.toFixed (/ b 1024 1024 1024) 2) "gb")
      :else ">1tb")))

(defn download-item
  [[filename file] owner]
  (dom/li nil
    filename " "
    (->bytes-display (get file :bytes-received 0)) " of "
    (->bytes-display (:total-bytes file)) " in "
    (:total-segments file) " segments: "
    (name (:status file))))

(defmulti worker-item (fn [[_ w] _] (:status w)))

(defmethod worker-item :default
  [[_ w] owner]
  (dom/li #js {:className "waiting"}
    ))

(defmethod worker-item :downloading
  [[_ w] owner]
  (dom/li #js {:className "orange-bg"}
    ))

(defmethod worker-item :error
  [[_ w] owner]
  (dom/li #js {:className "error"}
    ))

(defn key-info
  []
  (dom/ul #js {:id "key list-unstyled"}
    (dom/li #js {:className "label label-default"}
      "Waiting")
    (dom/li #js {:className "label label-primary"}
      "Working")
    (dom/li #js {:className "label label-success"}
      "Success")
    (dom/li #js {:className "label label-danger"}
      "Error")))

(defn leacher-app
  [{:keys [downloads workers] :as app} owner]
  (reify
    om/IWillMount
    (will-mount [this]
      (let [ws (ws-chan)]
        (go
          (loop []
            (let [e (<! ws)]
              (handle-event e)
              (recur))))))
    om/IRender
    (render [this]
      (dom/div #js {:className "container"}
        (dom/div #js {:className "row"}
          (dom/div #js {:className "col-md-12"}
            (key-info)))
        (dom/div #js {:className "row"}
          (dom/div #js {:className "col-md-12"}
            (dom/h2 nil "Leaching "
              (dom/span #js {:className "small"}
                (dom/span #js {:className "badge"}
                  (count workers)) " connections"))
            (apply dom/ul #js {:className "list-unstyled" :id "workers"}
              (om/build-all worker-item workers))))
        (dom/div #js {:className "clearfix"})

        (dom/div #js {:className "row"}
          (dom/div #js {:className "col-md-12"}
            (dom/h2 nil "Downloads "
              (dom/span #js {:className "small"}
                "All your illegal files"))
            (apply dom/ul #js {:id "downloads"}
              (om/build-all download-item downloads))))))))

(om/root leacher-app app-state
  {:target (.getElementById js/document "app")})

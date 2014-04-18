(ns leacher.ui.app
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [goog.events :as events]
            [cljs.core.async :refer [put! <! chan]]
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
  (println data)
  (swap! app-state merge (:data data)))

(defn download-item
  [download owner]
  (dom/li nil
    (:filename download)))

(defmulti worker-item (fn [[_ w] _] (:status w)))

(defmethod worker-item :waiting
  [[_ w] owner]
  (dom/li nil
    "Waiting..."))

(defmethod worker-item :downloading
  [[_ w] owner]
  (dom/li nil
    "Downloading " (:filename w) "/" (:message-id w)))

(defmethod worker-item :error
  [[_ w] owner]
  (dom/li #js {:className "error"}
    "Error: " (:message w)))

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
      (dom/div nil
        (dom/h1 nil "Leacher")
        (dom/h2 nil "Workers"
          (apply dom/ul #js {:id "workers"}
            (om/build-all worker-item workers)))
        (dom/h2 nil "Downloads")
        (apply dom/ul #js {:id "downloads"}
          (om/build-all download-item downloads))))))

(om/root leacher-app app-state
  {:target (.getElementById js/document "app")})

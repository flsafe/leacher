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

(def app-state (atom {:downloads {}
                      :workers   {}}))

(def status->cls
  {:downloading :warning
   :completed   :success
   :error       :danger
   :decoding    :primary
   :cleaning    :info})

;; websocket stuff

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

(defmulti handle-event :type)

(defmethod handle-event :default
  [{:keys [type]}]
  (println "ws" (name type)))

(defmethod handle-event :message
  [{:keys [data]}]
  (swap! app-state merge (:data data)))

;; utils

(defn ->bytes-display
  [b]
  (when b
    (cond
      (< b 1024)     (str b "b")
      (< b 1048576)  (str (.toFixed (/ b 1024) 2) "kb")
      (< b 1.074e+9) (str (.toFixed (/ b 1024 1024) 2) "mb")
      (< b 1.1e+12)  (str (.toFixed (/ b 1024 1024 1024) 2) "gb")
      :else ">1tb")))

(defn label
  [cls text & {:as attrs}]
  (dom/span #js {:className (str "label label-" (name cls))}
    text))

(defn badge
  [text]
  (dom/span #js {:className "badge"}
    text))

;; page elements

(defn download-item
  [[filename file] owner]
  (dom/li nil
    (dom/h4 nil
      filename " "
      (dom/span #js {:className "small"}
        (->bytes-display (:bytes-received file)) "/"
        (->bytes-display (:total-bytes file))))
    (dom/div #js {:className "status"}
      (label (get status->cls (:status file) :default)
        (-> file :status name)))
    (dom/p nil
      "in "
      (dom/span #js {:className "groups"}
        (string/join ", " (:groups file)))
      " posted by "
      (dom/span #js {:className "poster"}
        (:poster file))
      " with "
      (dom/span #js {:className "segments"}
        (:segments-completed file) "/" (:total-segments file))
      " segments")
    ;; (->bytes-display (get file :bytes-received 0)) " of "
    ;; (->bytes-display (:total-bytes file)) " in "
    ;; (badge (:total-segments file)) " segments"
    ))

(defn downloads-section
  [downloads]
  (dom/div #js {:className "col-md-10"}
    (dom/h2 nil "Downloads "
      (dom/span #js {:className "small"}
        "All your illegal files"))
    (apply dom/ul #js {:id "downloads" :className "list-unstyled"}
      (om/build-all download-item downloads))))

(defn worker-item
  [[_ w] owner]
  (dom/li #js {:className (-> w :status name)}))

(defn workers-section
  [workers]
  (dom/div #js {:className "col-md-2"}
    (dom/ul #js {:className "list-unstyled"
                 :id "key"}
      (dom/li nil
        (dom/span #js {:className "label waiting"}
          "Waiting"))
      (dom/li nil
        (dom/span #js {:className "label downloading"}
          "Downloading"))
      (dom/li nil
        (dom/span #js {:className "label pinging"}
          "Pinging"))
      (dom/li nil
        (dom/span #js {:className "label fatal"}
          "Fatal")))
    (apply dom/ul #js {:className "list-unstyled"
                       :id "workers"}
      (om/build-all worker-item workers))))

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
          (workers-section workers)
          (downloads-section downloads))))))

(om/root leacher-app app-state
  {:target (.getElementById js/document "app")})

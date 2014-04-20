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
      (< b 1048576)  (str (.toFixed (/ b 1024) 0) "kb")
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

(defn time-diff
  [from to]
  (let [start    (if from
                   (.moment js/window from)
                   (js/moment))
        finish   (if to
                   (.moment js/window to)
                   (js/moment))
        duration (.duration js/moment (.subtract finish start))
        seconds  (.as duration "seconds")
        human    (if (< seconds 60)
                   (str seconds " seconds")
                   (.humanize duration))]
    {:start    start
     :finish   finish
     :duration duration
     :human    human}))

(defn download-item
  [[filename file] owner]
  (let [completed?      (= :completed (:status file))
        running-time    (time-diff (:started-at file) (:finished-at file))
        bytes-received  (:bytes-received file)
        total-bytes     (:total-bytes file)
        running-secs    (.as (:duration running-time) "seconds")
        rate            (/ bytes-received running-secs)
        decoding-time   (time-diff (:decoding-started-at file) (:decoding-finished-at file))]
    (dom/li nil
      (dom/h4 nil
        filename " "
        (dom/span #js {:className "small"}
          (->bytes-display bytes-received) "/"
          (->bytes-display total-bytes)
          " @ "
          (->bytes-display rate) "/s"))
      (dom/div #js {:className "status"}
        (label (get status->cls (:status file) :default)
          (-> file :status name)))
      (dom/p nil
        "in "
        (dom/span #js {:className "data"}
          (string/join ", " (:groups file)))
        ", posted by "
        (dom/span #js {:className "data"}
          (:poster file)))
      (apply dom/p nil
        " with "
        (dom/span #js {:className "data"}
          (:segments-completed file) "/" (:total-segments file))
        " segments, "
        (into (if completed?
                ["completed in "
                 (dom/span #js {:className "data"}
                   (:human running-time))
                 ", decoded in "
                 (dom/span #js {:className "data"}
                   (:human decoding-time))]
                ["running for "
                 (dom/span #js {:className "data"}
                   (:human running-time))]))))))

(defn downloads-section
  [downloads]
  (dom/div #js {:className "row"}
    (dom/div #js {:className "col-md-12"}
      (dom/h3 nil "Downloads "
        (dom/span #js {:className "small"}
          "All your illegal files"))
      (apply dom/ul #js {:id "downloads" :className "list-unstyled"}
        (om/build-all download-item downloads)))))

(defn worker-item
  [[_ w] owner]
  (dom/li #js {:className (-> w :status name)}))

(defn workers-section
  [workers]
  (dom/div #js {:className "row"}
    (dom/div #js {:className "col-md-12"}
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
        (om/build-all worker-item workers))
      (dom/div #js {:className "clearfix"})
      )))

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

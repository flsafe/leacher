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

;; state

(def config (atom {}))

(def app-state (atom {:websocket :disconnected
                      :downloads {}
                      :workers   {}}))

;; ws handling

(defn deep-merge-with
  [f & maps]
  (apply
   (fn m [& maps]
     (if (every? map? maps)
       (apply merge-with m maps)
       (apply f maps)))
   maps))

(def deep-merge (partial deep-merge-with (fn [& ms] (last ms))))

(defn apply-removals
  [m deltas]
  (reduce (fn [res korks]
            (if (vector? korks)
              (let [[f & r] korks
                    val     (apply-removals (get res f) (vec r))]
                (if (empty? val)
                  (dissoc res f)
                  (assoc res f val)))
              (dissoc res korks))) m deltas))

(defn apply-deltas
  [m deltas]
  ;; (.time js/console "apply-deltas")
  (let [rm (:remove deltas)
        md (:modify deltas)
        res (apply-removals m rm)
        res (if md
              (deep-merge res md)
              res)]
    ;; (.timeEnd js/console "apply-deltas")
    res))

(defn ws-chan
  []
  (let [c   (chan)
        ws  (doto (goog.net.WebSocket.)
              (goog.events.listen Events/OPENED
                (fn [e] (put! c {:type :opened :event e})))
              (goog.events.listen Events/CLOSED
                (fn [e] ))
              (goog.events.listen Events/ERROR
                (fn [e] (put! c {:type :error :event e})))
              (goog.events.listen Events/MESSAGE
                (fn [e]
                  ;; (.time js/console "read-string")
                  (let [data (cljs.reader/read-string (.-message e))]
                    ;; (.timeEnd js/console "read-string")
                    (put! c {:type :message :event e :data data})))))]
    (.open ws (str "ws://localhost:" (-> @config :ws-server :port) "/ws"))
    c))

(defmulti handle-message :type)

(defmethod handle-message :initial
  [{:keys [data]}]
  (reset! app-state data))

(defmethod handle-message :deltas
  [{:keys [data]}]
  (swap! app-state apply-deltas data))

(defmulti handle-event :type)

(defmethod handle-event :default
  [{:keys [type]}]
  (println "unhandled ws event" (name type)))

(defmethod handle-event :message
  [{:keys [data]}]
  (handle-message data))

;; page elements

(def status->cls
  {:downloading       :warning
   :completed         :success
   :error             :danger
   :decoding          :primary
   :cleaning          :info})

(def music-ext   #{"mp3" "m4p" "flac" "ogg"})
(def video-ext   #{"avi" "mp4" "mpg" "mkv"})
(def archive-ext #{"zip" "rar" "tar" "gz" "par" "par2"})

(defn file->glyphicon
  [filename]
  (let [[_  ext] (re-find  #"\.(.+)$" filename)]
    (condp contains? ext
      music-ext   "headphones"
      video-ext   "film"
      archive-ext "compressed"
      :nil)))

(defn ->bytes-display
  [b]
  (when b
    (cond
      (< b 1024)     (str (.toFixed b 0) "b")
      (< b 1048576)  (str (.toFixed (/ b 1024) 0) "kb")
      (< b 1.074e+9) (str (.toFixed (/ b 1024 1024) 2) "mb")
      (< b 1.1e+12)  (str (.toFixed (/ b 1024 1024 1024) 2) "gb")
      :else
      ">1tb")))

(defn label
  [cls text & {:as attrs}]
  (dom/span #js {:className (str "label label-" (name cls))}
    text))

(defn badge
  [text]
  (dom/span #js {:className "badge"}
    text))

(defn time-diff
  [from to]
  (let [start    (if from
                   (.moment js/window from)
                   (js/moment))
        finish   (if to
                   (.moment js/window to)
                   (js/moment))
        duration (.duration js/moment (.subtract finish start))
        seconds  (.asSeconds duration)
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
        running-secs    (.asSeconds (:duration running-time))
        rate            (/ bytes-received running-secs)
        decoding-time   (time-diff (:decoding-started-at file)
                          (:decoding-finished-at file))]
    (dom/li nil
      (dom/h4 nil
        (when-let [s (file->glyphicon filename)]
          (dom/span #js {:className (str "glyphicon glyphicon-" s)}))
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
        (condp = (:status file)
          :completed
          ["completed in "
           (dom/span #js {:className "data"}
             (:human running-time))
           ", decoded in "
           (dom/span #js {:className "data"}
             (:human decoding-time))]

          :working
          ["running for "
           (dom/span #js {:className "data"}
             (:human running-time))]

          nil)))))

(defn downloads-section
  [downloads]
  (dom/div #js {:className "row"}
    (dom/div #js {:className "col-md-12"}
      (dom/h3 nil "Downloads "
        (dom/span #js {:className "small"}
          "All your illegal files"))
      (if (zero? (count downloads))
        (dom/p nil "No downloads? You're not trying hard enough")
        (apply dom/ul #js {:id "downloads" :className "list-unstyled"}
          (om/build-all download-item downloads))))))

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
      (dom/div #js {:className "clearfix"}))))

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

(defn init
  [cfg]
  (reset! config (read-string cfg))
  (om/root leacher-app app-state
    {:target (.getElementById js/document "app")}))

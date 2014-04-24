(ns leacher.ui.app
  (:require-macros [cljs.core.async.macros :refer [go go-loop]])
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
  (let [rm (:remove deltas)
        md (:modify deltas)
        res (apply-removals m rm)
        res (if md
              (deep-merge res md)
              res)]
    res))

(defn ws-chan
  []
  (let [in  (chan)
        out (chan)
        ws  (doto (goog.net.WebSocket.)
              (goog.events.listen Events/OPENED
                (fn [e]
                  (put! out {:type :opened :event e})))
              (goog.events.listen Events/CLOSED
                (fn [e]
                  (put! out {:type :closed :event e})))
              (goog.events.listen Events/ERROR
                (fn [e]
                  (put! out {:type :error :event e})))
              (goog.events.listen Events/MESSAGE
                (fn [e]
                  (let [data (cljs.reader/read-string (.-message e))]
                    (put! out {:type :message :event e :data data})))))]
    (go-loop []
      (when-let [m (<! in)]
        (.send ws (pr-str m))
        (recur)))
    (.open ws (str "ws://localhost:" (-> @config :ws-server :port) "/ws"))
    {:in in :out out}))

(defmulti handle-message :type)

(defmethod handle-message :initial
  [{:keys [data]}]
  (swap! app-state merge data))

(defmethod handle-message :deltas
  [{:keys [data]}]
  (swap! app-state apply-deltas data))

(defmulti handle-event :type)

(defmethod handle-event :opened
  [_]
  (swap! app-state assoc :websocket :connected))

(defmethod handle-event :closed
  [_]
  (swap! app-state assoc :websocket :disconnected))

(defmethod handle-event :error
  [_]
  (swap! app-state assoc :websocket :error))

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
  (let [ext (last (string/split filename #"\."))]
    (condp contains? ext
      music-ext   "headphones"
      video-ext   "film"
      archive-ext "compressed"
      :nil)))

(defn ->bytes-display
  [b]
  (when b
    (cond
      (< b 1024)     (str (.toFixed b 0) " B")
      (< b 1048576)  (str (.toFixed (/ b 1024) 0) " KB")
      (< b 1.074e+9) (str (.toFixed (/ b 1024 1024) 2) " MB")
      (< b 1.1e+12)  (str (.toFixed (/ b 1024 1024 1024) 2) " GB")
      :else
      ">1 TB")))

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
  (let [completed?       (= :completed (:status file))
        running-time     (time-diff (:started-at file) (:finished-at file))
        bytes-received   (:bytes-received file)
        total-bytes      (:total-bytes file)
        running-secs     (.asSeconds (:duration running-time))
        rate             (/ bytes-received running-secs)
        decoding-time    (time-diff (:decoding-started-at file)
                           (:decoding-finished-at file))
        percent-complete (int (* 100 (/ bytes-received total-bytes)))]
    (dom/li nil
      (dom/div #js {:className "icon pull-left"}
        (when-let [s (file->glyphicon filename)]
          (dom/span #js {:className (str "glyphicon glyphicon-" s)})))
      (dom/div #js {:className "pull-left"}
        (dom/span #js {:className "filename"}
          filename)
        (dom/div nil
          (dom/span #js {:className "bytes"}
            (->bytes-display bytes-received) " of "
            (->bytes-display total-bytes))
          " - "
          (dom/span #js {:className "rate"}
            (->bytes-display rate) "/sec")
          (when completed?
            (dom/span #js {:className "timing"}
              ", downloaded in "
              (dom/span #js {:className "data"}
                (:human running-time))
              ", decoded in "
              (dom/span #js {:className "data"}
                (:human decoding-time))))))

      (when-not completed?
        (dom/div #js {:className "progress pull-right"}
          (dom/div #js {:className "progress-bar"
                        :style     #js {:width (str percent-complete "%")}}
            (str percent-complete "%"))))

      (when completed?
        (dom/div #js {:className "status pull-right"}
          (label (get status->cls (:status file) :default)
            (-> file :status name))))

      (dom/div #js {:className "clearfix"}))))

(defn downloads-section
  [downloads ws-in]
  (dom/div #js {:className "row"}
    (dom/div #js {:className "col-md-12"}
      (dom/h3 #js {:className "pull-left"}
        "Downloads "
        (dom/span #js {:className "small"}
          "All your illegal files"))
      (dom/button #js {:className "btn btn-xs pull-right clear-completed"
                       :onClick (fn [_] (put! ws-in {:type :clear-completed}))}
        "Clear completed")
      (dom/div #js {:className "clearfix"})
      (if (zero? (count downloads))
        (dom/p nil "No downloads? You're not trying hard enough")
        (apply dom/ul #js {:id "downloads"
                           :className "list-unstyled"}
          (om/build-all download-item downloads))))))

(defn worker-item
  [[_ w] owner]
  (dom/li #js {:className (-> w :status name)}))

(defn workers-section
  [workers]
  (dom/div #js {:className "col-md-12"}
    (apply dom/ul #js {:className "list-unstyled pull-left"
                       :id        "workers"}
      (om/build-all worker-item workers))
    (dom/div #js {:className "clearfix"})))

(defn connection-status
  [status]
  (dom/div #js {:className "col-md-12"}
    (dom/div #js {:id        "connection-status"
                  :className (str "pull-right " (name status))}
      (name status))))

(defn leacher-app
  [{:keys [downloads workers websocket] :as app} owner]
  (reify
    om/IInitState
    (init-state [_]
      {:ws nil})

    om/IWillMount
    (will-mount [this]
      (let [{:keys [in out] :as ws} (ws-chan)]
        (om/set-state! owner :ws ws)
        (go-loop []
          (when-let [e (<! out)]
            (handle-event e)
            (recur)))))

    om/IRenderState
    (render-state [this {:keys [ws]}]
      (dom/div #js {:className "container-fluid"}
        (dom/div #js {:className "row"}
          (connection-status websocket)
          (workers-section workers))
        (downloads-section downloads (:in ws))))))

(defn init
  [cfg]
  (reset! config (read-string cfg))
  (om/root leacher-app app-state
    {:target (.getElementById js/document "app")}))

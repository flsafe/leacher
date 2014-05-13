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
                      :settings  {}
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
              (let [[k attrs] korks
                    val     (apply-removals (get res k) attrs)]
                (if (empty? val)
                  (dissoc res k)
                  (assoc res k val)))
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
                    (println data)
                    (put! out {:type :message :event e :data data})))))]
    (go-loop []
      (when-let [m (<! in)]
        (println "sending:" m)
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
   :failed            :danger
   :decoding          :primary
   :cleaning          :info})

(defn file->glyphicon
  [filename]
  (let [ext (last (string/split filename #"\."))]
    (condp re-find ext
      #"\.(mp3|m4p|flac|ogg)"              "headphones"
      #"\.(avi|mp4|mpg|mkv)"               "film"
      #"\.(zip|rar|tar|gz|par|par2|r\d\d)" "compressed"
      :nil)))

(defn ->bytes-display
  [b]
  (cond
    (nil? b)          "0 B"
    (= js/Infinity b) "?"
    (< b 1024)        (str (.toFixed b 0) " B")
    (< b 1048576)     (str (.toFixed (/ b 1024) 0) " KB")
    (< b 1.074e+9)    (str (.toFixed (/ b 1024 1024) 2) " MB")
    (< b 1.1e+12)     (str (.toFixed (/ b 1024 1024 1024) 2) " GB")
    :else             ">1 TB"))

(defn label
  [cls text & {:as attrs}]
  (dom/span (clj->js (merge {:className (str "label label-" (name cls))}
                       attrs))
    text))

(defn badge
  [text]
  (dom/span #js {:className "badge"}
    text))

(defn or-now
  [t]
  (if t
    (.moment js/window t)
    (js/moment)))

(defn time-diff
  [from to]
  (let [start    (or-now from)
        finish   (or-now to)
        duration (.duration js/moment (.subtract finish start))
        seconds  (.asSeconds duration)
        human    (if (< seconds 60)
                   (str seconds " seconds")
                   (.humanize duration))]
    {:start    start
     :finish   finish
     :seconds  seconds
     :duration duration
     :human    human}))

(defn download-item
  [[filename file] owner]
  (let [completed?       (= :completed (:status file))
        downloading-time (time-diff (:downloading-started-at file)
                           (:downloading-finished-at file))
        total-time       (time-diff (:downloading-started-at file)
                           (:cleaning-finished-at file))
        bytes-received   (:bytes-received file)
        total-bytes      (:total-bytes file)
        rate             (/ bytes-received (:seconds downloading-time))
        decoding-time    (time-diff (:decoding-started-at file)
                           (:decoding-finished-at file))
        percent-complete (int (* 100 (/ bytes-received total-bytes)))]
    (dom/li #js {:className (if (:cancelled file) "cancelled")}
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
          ", "
          (dom/span #js {:className "segments"}
            (get file :segments-completed 0) "/"
            (:total-segments file))
          " segments"
          (when completed?
            (dom/span #js {:className "timing"}
              ", downloaded in "
              (dom/span #js {:className "data"}
                (:human downloading-time))
              ", decoded in "
              (dom/span #js {:className "data"}
                (:human decoding-time))))))

      (dom/div #js {:className "status pull-right"}
        (label (get status->cls (:status file) :default)
          (-> file :status name)
          :title (:error file)))

      (when (= :downloading (:status file))
        (dom/div #js {:className "progress"}
          (dom/div #js {:className "progress-bar"
                        :style     #js {:width (str percent-complete "%")}})))
      (dom/div #js {:className "clearfix"}))))

(defn downloads-section
  [downloads ws-in]
  (dom/div #js {:className "row"}
    (dom/div #js {:className "header col-md-12"}
      (dom/h3 #js {:className "pull-left"}
        "Downloads "
        (dom/span #js {:className "small"}
          "All your illegal files"))
      (dom/button #js {:className "btn btn-xs pull-right"
                       :onClick (fn [_] (put! ws-in {:type :clear-completed}))}
        "Clear completed")
      (dom/button #js {:className "btn btn-xs btn-danger pull-right"
                       :onClick (fn [_] (put! ws-in {:type :cancel-all}))}
        "Cancel all")

      (dom/div #js {:className "clearfix"})
      (if (zero? (count downloads))
        (dom/p nil "No downloads!")
        (apply dom/ul #js {:id "downloads"
                           :className "list-unstyled"}
          (om/build-all download-item downloads))))))

(defn worker-item
  [[_ w] owner]
  (dom/li #js {:className (-> w :status name)
               :title     (:message w)}))

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

(defn handle-change
  [e key settings parse-fn]
  (let [value (parse-fn (.. e -target -value))]
    (om/update! settings key value)))

(defn input-with-label
  [title key settings & [attrs]]
  (let [wrapper-class (get attrs :wrapper-class "form-group")
        parse-fn      (get attrs :parse-fn identity)
        attrs         (dissoc attrs :wrapper-class)]
   (dom/div #js {:className wrapper-class}
     (dom/label nil
       title)
     (dom/input
       (clj->js (merge {:type      "text"
                        :className "form-control input-sm"
                        :value     (get settings key)
                        :onChange  #(handle-change % key settings parse-fn)}
                  attrs))))))

(defn parse-int
  [s]
  (.parseInt js/window s))

(defn settings-editor
  [ws-msgs settings]
  (dom/div #js {:className "col-xs-2"}
    (input-with-label "Host" :host settings)
    (input-with-label "Port" :port settings {:parse-fn parse-int})
    (input-with-label "User" :user settings)
    (input-with-label "Password" :password settings {:type "password"})
    (input-with-label "SSL" :ssl? settings {:wrapper-class "checkbox" :type "checkbox" :className ""})
    (input-with-label "Max. Connections"
      :max-connections settings
      {:parse-fn parse-int})
    (dom/button #js {:className "btn btn-xs btn-success"
                     :onClick   #(put! ws-msgs {:type     :settings-update
                                                :settings @settings})}
      "Save")
    (dom/button #js {:className "btn btn-xs btn-warning"
                     :onClick   #(put! ws-msgs {:type     :settings-test
                                                :settings @settings})}
      "Test")))

(defn leacher-app
  [{:keys [downloads settings workers websocket] :as app} owner]
  (reify
    om/IInitState
    (init-state [_]
      {:ws      nil
       :ws-msgs (chan)})

    om/IWillMount
    (will-mount [this]
      (let [{:keys [in out] :as ws} (ws-chan)]
        (om/set-state! owner :ws ws)
        (go-loop []
          (when-let [e (<! out)]
            (handle-event e)
            (recur)))
        (go-loop []
          (when-let [msg (<! (om/get-state owner :ws-msgs))]
            (>! in msg)
            (recur)))))

    om/IRenderState
    (render-state [this {:keys [ws ws-msgs]}]
      (dom/div #js {:className "container-fluid"}
        (settings-editor ws-msgs settings)
        (dom/div #js {:className "col-xs-10"}
          (dom/div #js {:className "row"}
            (connection-status websocket)
            (workers-section workers))
          (downloads-section downloads (:in ws)))))))

(defn init
  [cfg]
  (reset! config (read-string cfg))
  (om/root leacher-app app-state
    {:target (.getElementById js/document "app")}))

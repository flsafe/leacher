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
                      :files     {}}))

;; ws handling

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
    (.open ws (str "ws://localhost:8091/ws"))
    {:in in :out out}))

(defn handle-update
  [state {:keys [type file filename status] :as data}]
  (condp = type
    :download-pending
    (swap! state assoc-in [:files (:filename file)]
      (-> file
        (assoc :status :pending
               :downloaded-segments 0
               :decoded-segments 0
               :download-failed-segments 0
               :decode-failed-segments 0
               :errors [])
        (dissoc :segments)))

    :file-status
    (swap! state assoc-in [:files filename :status] status)

    :segment-download-complete
    (swap! state update-in [:files filename :downloaded-segments] inc)

    :segment-download-failed
    (swap! state update-in [:files filename]
      (fn [m] (-> m
               (update-in [:download-failed-segments] inc)
               (update-in [:errors] conj (:message data)))))

    :segment-decode-complete
    (swap! state update-in [:files filename :decoded-segments] inc)

    :segment-decode-failed
    (swap! state update-in [:files filename]
      (fn [m] (-> m
               (update-in [:decode-failed-segments] inc)
               (update-in [:errors] conj (:message data)))))))

(defn handle-event
  [{:keys [type data] :as e}]
  (condp = type
    :opened
    (swap! app-state assoc :websocket :connected)

    :closed
    (swap! app-state assoc :websocket :disconnected)

    :error
    (swap! app-state assoc :websocket :error)

    :message
    (condp = (:type data)
      :initial
      (swap! app-state merge (:data data))

      :update
      (handle-update app-state (:data data)))))

;; page elements

(def status->cls
  {:downloading :warning
   :completed   :success
   :error       :danger
   :failed      :danger
   :decoding    :primary
   :cleaning    :info})

(defn file->glyphicon
  [filename]
  (let [ext (last (string/split filename #"\."))]
    (condp re-find ext
      #"(?i)(mp3|m4p|flac|ogg)"      "headphones"
      #"(?i)(avi|mp4|mpg|mkv)"       "film"
      #"(?i)(zip|rar|tar|gz|r\d\d)"  "compressed"
      #"(?i)(jpg|jpeg|gif|png|tiff)" "camera"
      #"(?i)(par|par2)"              "wrench"
      nil)))

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

(defn connection-status
  [status]
  (dom/div #js {:id        "connection-status"
                :className (name status)}))

(defn file-row
  [[filename file] _]
  (reify
    om/IRender
    (render [_]
      (let [success? (and (zero? (:decode-failed-segments file))
                       (zero? (:download-failed-segments file)))]
        (dom/li nil

          (dom/div #js {:className "icon pull-left"}
            (when-let [s (file->glyphicon filename)]
              (dom/span #js {:className (str "glyphicon glyphicon-" s)})))

          (dom/div #js {:className "pull-left"}
            (dom/span #js {:className "filename"}
              filename)

            (dom/div nil
              (dom/span #js {:className "segments"}
                (get file :downloaded-segments) "/"
                (:total-segments file))
              " segments downloaded, "
              (dom/span #js {:className "segments"}
                (get file :decoded-segments) "/"
                (:total-segments file))
              " segments decoded"))

          (dom/div #js {:className "status pull-right"}
            (label (get status->cls (:status file) :default)
              (-> file :status name)
              :title (:error file)))
         (dom/div #js {:className "clearfix"}))))))

(defn files-section
  [files _]
  (reify
    om/IRender
    (render [_]
      (dom/div #js {:className "col-md-6"}
        (dom/h2 nil "Files")
        (apply dom/ul #js {:id        "files"
                           :className "list-unstyled"}
          (om/build-all file-row (sort files)))))))

(defn leacher-app
  [{:keys [files settings websocket] :as app} owner]
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
            (recur)))))

    om/IRenderState
    (render-state [this {:keys [ws ws-msgs]}]
      (dom/div nil
        (connection-status websocket)
        (dom/div #js {:className "container-fluid"}
          (dom/div #js {:className "row"}
            (om/build files-section files)))))))

(defn init
  []
  (om/root leacher-app app-state
    {:target (.getElementById js/document "app")}))

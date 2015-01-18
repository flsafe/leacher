(ns leacher.ui.app
  (:require-macros [cljs.core.async.macros :refer [go go-loop]])
  (:require [goog.events :as events]
            [cljs.core.async :refer [put! <! chan close!]]
            [reagent.core :as reagent :refer [atom]]
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
                      :workers   []
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
               (update-in [:errors] conj (:message data)))))

    :worker-status
    (swap! state assoc-in [:workers (:worker data)] (:status data))))

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
  {:downloading     :warning
   :completed       :success
   :download-errors :danger
   :error           :danger
   :failed          :danger
   :decoding        :primary
   :cleaning        :info})

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
  [:span (merge attrs :class (str "label label-" (name cls)))
   text])

(defn connection-status
  []
  [:div#connection-status {:class (name (@app-state :websocket))}])

(defn file-row
  [filename file]
  (let [success? (and (zero? (:decode-failed-segments file))
                   (zero? (:download-failed-segments file)))]
    [:li

     [:div.icon.pull-left
      (when-let [s (file->glyphicon filename)]
        [:span {:class (str "glyphicon glyphicon-" s)}])]

     [:div.pull-left
      [:div.filename filename]
      [:div (:downloaded-segments file) "/" (:total-segments file)
       (when (> (:download-failed-segments file) 0)
         [:span.failures (str " (" (:download-failed-segments file) " failed)")])]]

     [:div.status.pull-right
      [:span {:class (str "label label-" (name (get status->cls (:status file) :default)))
              :title (:error file)}
       (-> file :status name)]]

     [:div.clearfix]]))

(defn files-section
  [files]
  [:div.col-md-6
   [:h4 "Files"]
   (if (zero? (count files))
     [:p "No files to show, start downloading something!"]
     [:ul#files.list-unstyled
      (for [[filename file] (sort files)]
        ^{:key filename} [file-row filename file])])])

(defn settings-section
  [settings]
  [:div.col-md-4
   [:dl#settings
    [:dt "Host"]
    [:dd (:host settings)]

    [:dt "Port"]
    [:dd (:port settings)]

    [:dt "User"]
    [:dd (:user settings)]

    [:dt "SSL?"]
    [:dd (str (:ssl? settings))]

    [:dt "Max Connections"]
    [:dd (str (:max-connections settings))]]])

(defn workers-section
  []
  [:div.col-md-12
   [:ul#workers.list-unstyled
    (for [worker (@app-state :workers)]
      [:li {:class (name worker)}])]
   [:div.clearfix]])

(defn leacher-app
  []
  (let [{:keys [in out]} (ws-chan)
        ws-msgs          (chan)]
    (go-loop []
      (when-let [e (<! out)]
        (handle-event e)
        (recur)))
    (fn []
      [:div
       [connection-status app-state]
       [:div.container-fluid
        [:div.row [workers-section]]
        [:div.row
         [settings-section (@app-state :settings)]
         [files-section (@app-state :files)]]]])))

(defn ^:export run
  []
  (reagent/render-component [leacher-app]
    (.getElementById js/document "app")))

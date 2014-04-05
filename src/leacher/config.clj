(ns leacher.config)

(defonce home-dir
  (str (System/getProperty "user.home") "/.leacher"))

(def template
  {:nntp            {:host     ""
                     :port     119
                     :user     ""
                     :password ""
                     :max-connections 20}
   :app-state       {:path (str home-dir "state.edn")}
   :dirs            {:complete (str home-dir "/complete")
                     :queue    (str home-dir "/queue")
                     :temp     (str home-dir "/tmp")}})

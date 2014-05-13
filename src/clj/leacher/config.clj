(ns leacher.config)

(defonce home-dir
  (str (System/getProperty "user.home") "/.leacher"))

(def template
  {:decoders           5
   :max-file-downloads 5
   :app-state          {:path     (str home-dir "/state.edn")}
   :dirs               {:complete (str home-dir "/complete")
                        :queue    (str home-dir "/queue")
                        :temp     (str home-dir "/tmp")}
   :http-server        {:port 8090}
   :ws-server          {:port 8091}})

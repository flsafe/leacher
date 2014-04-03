(defproject leacher "0.1.0-SNAPSHOT"
  :description "download from usenet"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/data.zip "0.1.1"]
                 [org.clojure/core.incubator "0.1.3"]
                 [org.clojure/tools.namespace "0.2.4"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [org.clojure/tools.logging "0.2.6"]
                 [me.raynes/fs "1.4.4"]
                 [clojure-lanterna "0.9.4"]
                 [com.stuartsierra/dependency "0.1.1"]
                 [com.stuartsierra/component "0.2.1"]
                 [log4j "1.2.17"]]
  :main leacher.main
  :global-vars {*warn-on-reflection* true})

(defproject leacher "0.1.0-SNAPSHOT"
  :description "download from usenet"
  :dependencies [
                 ;; clojure core
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/data.zip "0.1.1"]
                 [org.clojure/tools.namespace "0.2.4"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/data.json "0.2.4"]

                 ;; misc external
                 [me.raynes/fs "1.4.5"]
                 [clojure-lanterna "0.9.4"]

                 ;; blue haired stuart's libraries
                 [com.stuartsierra/dependency "0.1.1"]
                 [com.stuartsierra/component "0.2.1"]

                 ;; http stuff
                 [ring "1.2.2"]
                 [hiccup "1.0.5"]
                 [http-kit "2.1.18"]
                 [compojure "1.1.6"]

                 ;; cljs stuff
                 [org.clojure/clojurescript "0.0-2202"]
                 [om "0.5.3"]
                 [secretary "1.1.0"]

                 ;; omg logging
                 [log4j "1.2.17"]]

  :plugins [[lein-cljsbuild "1.0.3"]]

  :source-paths ["src/clj"]

  :main leacher.main

  :profiles {:dev     {:source-paths ["dev"]
                       :global-vars {*warn-on-reflection* true}}
             :prod    {:hooks [leiningen.cljsbuild]}
             :uberjar {:aot [leacher.main]}}

  :cljsbuild {:builds [{:id           "dev"
                        :source-paths ["src/cljs"]
                        :compiler     {:output-to     "resources/public/js/app.js"
                                       :output-dir    "resources/public/out-dev"
                                       :optimizations :whitespace}}]})

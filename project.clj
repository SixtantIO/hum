(defproject io.sixtant/hum "0.1.0"
  :description "An experimental library for efficient binary serialization of L2 book data."
  :dependencies [[org.clojure/clojure "1.10.1"]

                 [smee/binary "0.5.5"]

                 [com.taoensso/encore "3.8.0"]
                 [com.taoensso/timbre "5.1.0"]
                 [com.taoensso/tufte "2.2.0"]]
  :aliases {"bench" ["run" "-m" "io.sixtant.hum.benchmark"]
            "docs" ["run" "-m" "io.sixtant.hum.docs"]}
  :repl-options {:init-ns io.sixtant.hum})

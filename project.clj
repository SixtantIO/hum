(defproject io.sixtant.hum "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]

                 [smee/binary "0.5.5"]

                 [com.taoensso/encore "3.8.0"]
                 [com.taoensso/timbre "5.1.0"]
                 [com.taoensso/tufte "2.2.0"]]
  :repl-options {:init-ns io.sixtant.hum})

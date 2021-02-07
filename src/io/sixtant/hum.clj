(ns io.sixtant.hum
  (:require [io.sixtant.hum.codec1 :as codec1]
            [clojure.java.io :as io]
            [taoensso.tufte :as tufte])
  (:import (java.io OutputStream InputStream ByteArrayOutputStream Closeable)))


(set! *warn-on-reflection* true)


(defn l2-writer [^OutputStream out]
  (codec1/->L2Writer out (volatile! nil)))


(defn l2-reader [^InputStream in]
  (codec1/->L2Reader in (volatile! nil)))


(defn write-with [writer messages]
  (let [out (ByteArrayOutputStream.)]
    (with-open [^Closeable w (writer out)]
      (run! (fn [m] (tufte/p :write (w m))) messages)
      (.toByteArray out))))


(defn read-with [reader bytes]
  (let [in (io/input-stream bytes)]
    (with-open [^Closeable r (reader in)]
      (into [] (take-while some?) (repeatedly (fn [] (tufte/p :read (r))))))))

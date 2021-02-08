(ns io.sixtant.hum
  (:require [io.sixtant.hum.codec1 :as codec1]
            [io.sixtant.hum.codec2 :as codec2]
            [io.sixtant.hum.codec3 :as codec3]
            [clojure.java.io :as io]
            [taoensso.tufte :as tufte])
  (:import (java.io OutputStream InputStream ByteArrayOutputStream Closeable)))

(set! *warn-on-reflection* true)

(defn writer1 [^OutputStream out] (codec1/writer out))
(defn reader1 [^InputStream in] (codec1/reader in))

(defn writer2 [^OutputStream out] (codec2/writer out))
(defn reader2 [^InputStream in] (codec2/reader in))

(defn writer3 [^OutputStream out] (codec3/writer out))
(defn reader3 [^InputStream in] (codec3/reader in))

(defn write-with [writer messages]
  (let [out (ByteArrayOutputStream.)]
    (with-open [^Closeable w (writer out)]
      (run! (fn [m] (tufte/p :write (w m))) messages)
      (.toByteArray out))))

(defn read-with [reader bytes]
  (let [in (io/input-stream bytes)]
    (with-open [^Closeable r (reader in)]
      (into [] (take-while some?) (repeatedly (fn [] (tufte/p :read (r))))))))

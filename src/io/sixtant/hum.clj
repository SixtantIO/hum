(ns io.sixtant.hum
  "An experimental library for efficient binary serialization of L2 book data."
  (:require [clojure.java.io :as io]
            [io.sixtant.hum.arthur.codec :as arthur]
            [taoensso.tufte :as tufte])
  (:import (java.io OutputStream InputStream ByteArrayOutputStream Closeable)))


(set! *warn-on-reflection* true)


(defn writer
  "A writer for the ARTHUR codec that serializes messages to the OutputStream."
  [^OutputStream out]
  (arthur/writer out))


(defn reader
  "A reader for the ARTHUR codec that reads message from the InputStream."
  [^InputStream in] (arthur/reader in))


(defn write-with
  "Write `messages` with the given `writer` and return a byte array."
  [writer messages]
  (let [out (ByteArrayOutputStream.)]
    (with-open [^Closeable w (writer out)]
      ;; Call the writer as a function to write a message. Wrap with tufte
      ;; for profiling.
      (let [write-message (fn [m] (tufte/p :write (w m)))]
        (run! write-message messages))
      (.toByteArray out))))


(defn read-with
  "Eagerly read all messages from the given `bytes` using `reader`."
  [reader bytes]
  (let [in (io/input-stream bytes)]
    (with-open [^Closeable r (reader in)]
      ;; Similar to writing, call the reader as a function to read the next
      ;; message (or nil at EOF). Wrap with tufte for profiling.
      (let [try-read (fn [] (tufte/p :read (r)))]
        (into [] (take-while some?) (repeatedly try-read))))))

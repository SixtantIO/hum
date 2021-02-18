(ns io.sixtant.hum.arthur.codec
  "Expose a reader and writer for messages."
  (:require [io.sixtant.hum.arthur.message-frame :as frame]
            [io.sixtant.hum.arthur.serialize :as ser])
  (:import (java.io OutputStream Closeable InputStream)
           (clojure.lang IFn)))


(set! *warn-on-reflection* true)


(defrecord L2Writer [^OutputStream out context]
  Closeable
  (close [this]
    (vreset! context ::closed)
    (.flush out)
    (.close out))

  IFn
  (invoke [this message]
    (let [ctx @context
          _ (assert (not= ctx ::closed) "Writer is open.")
          ;; Build message frames / possibly update the context
          [ctx frames] (ser/frames message ctx)]
      (run! #(frame/write-frame % out) frames)
      (vreset! context ctx))
    true))


(defrecord L2Reader [^InputStream in context]
  Closeable
  (close [this]
    (vreset! context ::closed)
    (.close in))

  IFn
  (invoke [this]
    (let [ctx @context]
      (assert (not= ctx ::closed) "Reader is open.")
      (when-let [frame (frame/read-frame in)]
        (let [[ctx msg] (ser/message frame ctx)]
          (vreset! context ctx)

          ;; Timestamp messages are omitted, so just read the next message
          ;; after updating the context
          (if (= (:message-type-flag frame) frame/TIMESTAMP)
            (recur)
            msg))))))


(defn writer [out] (->L2Writer out (volatile! nil)))
(defn reader [in] (->L2Reader in (volatile! nil)))

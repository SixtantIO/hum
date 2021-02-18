(ns io.sixtant.hum.arthur.serialize
  "Functions to convert between messages and writable message frames."
  (:require [io.sixtant.hum.arthur.book-snapshot :as snap]
            [io.sixtant.hum.arthur.message-frame :as frame]
            [io.sixtant.hum.arthur.level-diff :as diff])
  (:import (io.sixtant.hum.messages OrderBookSnapshot OrderBookDiff)
           (java.io ByteArrayOutputStream ByteArrayInputStream NotSerializableException)))


(set! *warn-on-reflection* true)


(defprotocol Frameable
  (frame* [this context ts-offset] "Return [context message-frame]."))


(extend-protocol Frameable
  OrderBookSnapshot
  (frame* [this context ts-offset]
    (let [b (ByteArrayOutputStream.)
          ;; Each snapshot creates a new context
          context (snap/write-snapshot this context b)]
      [context
       (frame/frame frame/SNAPSHOT (.toByteArray b) ts-offset)]))

  OrderBookDiff
  (frame* [this context ts-offset]
    (let [{:keys [price qty bid? timestamp snapshot-delay]} this]
      (try
        (let [b (ByteArrayOutputStream.)
              removal? (zero? qty)]

          (if removal?
            (diff/write-removal price context b)
            (diff/write-diff price qty context b))

          [context
           (frame/frame
             (if removal?
               (if bid? frame/BID-REMOVAL frame/ASK-REMOVAL)
               (if bid? frame/BID-DIFF frame/ASK-DIFF))
             (.toByteArray b)
             ts-offset)])
        (catch NotSerializableException _
          (let [snap (some-> snapshot-delay deref)]
            (when-not snap
              (throw
                (IllegalArgumentException.
                  (str "Diffs must include a :snapshot-delay so that a fresh "
                       "snapshot can be written in case of overflow."))))
            (frame* ^Frameable snap context ts-offset)))))))


(defn- ?timestamp-frame [context timestamp]
  (if (> (- timestamp (get context :timestamp 0)) frame/max-ts-offset)
    ;; Context timestamp resets, and we write a timestamp message
    [(assoc context :timestamp timestamp)
     (frame/timestamp timestamp)]
    [context]))


(defn frames
  "Take a message and a serialization context, return `[context msg-frames]`."
  [message context]
  ;; First check if the timestamp offset is larger than the max ushort
  (let [ts (:timestamp message)
        [context ?ts-frame] (?timestamp-frame context ts)
        ts-offset (- ts (:timestamp context))

        ;; Build message frames for writing -- timestamp frame (if any) first
        [context frame] (frame* message context ts-offset)]
    [context
     (if ?ts-frame [?ts-frame frame] [frame])]))


(defn read-ts [ts-frame context]
  (let [ts (frame/read-timestamp ts-frame)]
    [(assoc context :timestamp (long ts)) ts]))


(defn- frame-bytes [f] (ByteArrayInputStream. (:message-bytes f)))


(defn read-snapshot [frame context]
  (let [[snap ctx] (snap/read-snapshot (frame-bytes frame))
        ctx (merge context ctx)
        ts (+ (:timestamp ctx) (:timestamp-offset frame))]
    [ctx (assoc snap :timestamp ts)]))


(defn read-diff [frame context bid?]
  (let [diff (diff/read-diff context bid? (frame-bytes frame))
        ts (+ (:timestamp context) (:timestamp-offset frame))]
    [context (assoc diff :timestamp ts)]))


(defn read-removal [frame context bid?]
  (let [diff (diff/read-removal context bid? (frame-bytes frame))
        ts (+ (:timestamp context) (:timestamp-offset frame))]
    [context (assoc diff :timestamp ts)]))


(defn message
  "Deserialize a message, return `[context msg]`."
  [frame context]
  (condp = (:message-type-flag frame)
    frame/TIMESTAMP   (read-ts frame context)
    frame/SNAPSHOT    (read-snapshot frame context)
    frame/ASK-DIFF    (read-diff frame context false)
    frame/ASK-REMOVAL (read-removal frame context false)
    frame/BID-DIFF    (read-diff frame context true)
    frame/BID-REMOVAL (read-removal frame context true)))

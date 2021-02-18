(ns io.sixtant.hum.codec.message-frame
  "The message frame inside of which all messages nest.

  The frame defines a message type number, a timestamp offset, and the message
  length in bytes.

  Each file produced by this codec starts with a 'timestamp' message, and each
  frame's timestamp offset is in reference to the timestamp contained in the
  most recently seen 'timestamp' message. This way, the message frame uses only
  2 bytes for the timestamp offset instead of 8 for a full timestamp.

  The message frame always begins with a 3-bit unsigned integer representing the
  message type. The next 5 bits are either an unsigned integer representing the
  message length for a 'compact' frame, or are set to zero in an 'extended'
  frame:

  - A compact message frame uses a single byte to represent both the message
    type and the message byte length:

          +--------+--------+---------+--------------+
          |  Type  | Length |    TS   |     Msg      |
          | 3 bits | 5 bits | 2 bytes | Length bytes |
          +--------+--------+---------+--------------+

  - An extended message frame uses an additional 4 bytes to represent the
    message byte length (e.g. for book snapshots, where the message might be
    very large):

          +--------+-----------+---------+---------+--------------+
          |  Type  | 0-Padding |  Length |    TS   |     Msg      |
          | 3 bits |   5 bits  | 4 bytes | 2 bytes | Length bytes |
          +--------+-----------+---------+---------+--------------+"
  (:require [io.sixtant.hum.codec.utils :as u]
            [org.clojars.smee.binary.core :as b]
            [taoensso.truss :as truss])
  (:import (java.io OutputStream InputStream EOFException ByteArrayOutputStream ByteArrayInputStream)))


(set! *warn-on-reflection* true)


;; Message types
(def ^:const TIMESTAMP   0)
(def ^:const SNAPSHOT    1)
(def ^:const ASK-DIFF    2)
(def ^:const ASK-REMOVAL 3)
(def ^:const BID-DIFF    4)
(def ^:const BID-REMOVAL 5)


(def ^:const message-type-bits 3)
(def ^:const message-length-bits 5)
(def ^:const max-type-flag (dec (u/pow2 3))) ; type is always 3 bits
(def ^:const max-compact-message-length (dec (u/pow2 message-length-bits)))
(def ^:const max-message-bytes (dec (u/pow2 32))) ; length is a uint
(def ^:const max-ts (dec (u/pow2 64))) ; ts is a ulong
(def ^:const max-ts-offset (dec (u/pow2 16))) ; ts offset is a ushort


;; The first byte is split: first 3 bits are msg type, next 5 are msg
;; length. If the message length doesn't fit, write '0' for the next 5 bits
;; and follow with a uint.
(def ^:private frame-prefix (u/uints message-type-bits message-length-bits))


(defrecord MessageFrame [message-type-flag message-byte-length timestamp-offset message-bytes])


(defn frame
  "Construct a message frame around the message bytes."
  [type-flag msg-bytes ts-offset]
  (truss/have #(<= 1 % max-message-bytes) (count msg-bytes))
  (truss/have #(<= % max-ts-offset) ts-offset)
  (truss/have #(<= % max-type-flag) type-flag)
  (->MessageFrame type-flag (count msg-bytes) ts-offset msg-bytes))


(defn write-frame
  "Serialize & write the message frame bytes."
  [frame ^OutputStream out]
  (let [{:keys [message-type-flag
                message-byte-length
                timestamp-offset
                message-bytes]} frame]
    ;; If the message type & length can be fit into a single byte, do so.
    ;; Otherwise, write the message type as one byte, and the length as four.
    (b/encode frame-prefix
              out
              [(biginteger message-type-flag)
               (if (<= message-byte-length max-compact-message-length)
                 (biginteger message-byte-length)
                 BigInteger/ZERO)])
    (when-not (<= message-byte-length max-compact-message-length)
      (b/encode :uint out message-byte-length))

    ;; Then 2 bytes for the timestamp offset
    (b/encode :ushort out timestamp-offset)

    ;; And finally the message data
    (.write ^OutputStream out ^bytes message-bytes)))


(defn- deserialize* [in]
  (let [[type len] (b/decode frame-prefix in)
        ;; if len is 0, then it isn't a compact frame, and we have to read a
        ;; 4-byte message length integer
        ml (if (zero? len) (b/decode :uint in) len)
        timestamp-offset (b/decode :ushort in)]
    (map->MessageFrame
      {:message-type-flag type
       :message-byte-length ml
       :timestamp-offset timestamp-offset
       :message-bytes (.readNBytes ^InputStream in ml)})))


(defn read-frame
  "Read the next message frame from the InputStream, or nil if EOF."
  [^InputStream in]
  (try (deserialize* in) (catch EOFException _ nil)))


(defn timestamp
  "A message frame which sets the reference timestamp for subsequent messages."
  [reference-timestamp]
  (let [b (ByteArrayOutputStream.)]
    (truss/have #(<= % max-ts) reference-timestamp)
    (b/encode :ulong b (bigint reference-timestamp))
    (frame TIMESTAMP (.toByteArray b) 0)))


(defn read-timestamp
  "Extract a timestamp from a timestamp message frame."
  [ts-frame]
  (b/decode :ulong (ByteArrayInputStream. (:message-bytes ts-frame))))

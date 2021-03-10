(ns io.sixtant.hum.arthur.level-diff
  "A change to an order book level.

  A level diff is either a [price qty] tuple to mean that the `price` level
  now has the new `qty`, or simply a `price` field to indicate a removal.

  Both numbers are represented as an integer number of ticks or lots according
  to the serialization context map. The number of ticks has a field length
  designated by the :pbits attribute of the serialization context map, and the
  number of lots is the rest of the message (the message length is specified in
  the message frame).

  A level diff:

      +-----------------+-------+
      |       Lots      | Ticks |
      | Variable Length | pbits |
      +-----------------+-------+

  A level removal:

      +-------+
      | Ticks |
      | pbits |
      +-------+

  The type of message (ask/bid and diff/removal) is interpreted from the
  message type flag in the surrounding message frame."
  (:require [io.sixtant.hum.utils :as u]
            [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum.uint :as uint])
  (:import (java.io OutputStream InputStream NotSerializableException)))


(set! *warn-on-reflection* true)


(defn write-diff
  "Write a single {:price _ :qty _} level diff."
  [price qty context ^OutputStream out]
  (let [{:keys [pbits tick-size lot-size]} context
        ticks (u/->ticks price tick-size)
        lots (u/->lots qty lot-size)]

    ;; Overflow
    (when (> (.bitLength ^BigInteger ticks) pbits)
      (throw (NotSerializableException.)))

    (let [packed (uint/pack-two-ints lots ticks pbits)
          ubytes (uint/uint->ubytes packed)]
      (.write out (byte-array (mapv b/ubyte->byte ubytes))))))


(defn read-diff
  "Read a single {:price _ :qty _} level diff."
  [context bid? ^InputStream in]
  (let [{:keys [pbits tick-size lot-size]} context
        bs (.readAllBytes in)
        ubytes (mapv b/byte->ubyte bs)
        packed (uint/ubytes->uint ubytes)
        [lots ticks] (uint/unpack-two-ints packed pbits)]
    (messages/map->OrderBookDiff
      {:price (* (bigdec ticks) tick-size)
       :qty (* (bigdec lots) lot-size)
       :bid? bid?})))


(defn write-removal
  "Write a single price field indicating level removal."
  [price context ^OutputStream out]
  (let [{:keys [pbits tick-size]} context
        ticks (u/->ticks price tick-size)]
    (b/encode (u/uints pbits) out [ticks])))


(defn read-removal
  "Read a single {:price _ :qty _} level diff."
  [context bid? ^InputStream in]
  (let [{:keys [pbits tick-size]} context
        [ticks] (b/decode (u/uints pbits) in)]
    (messages/map->OrderBookDiff
      {:price (* (bigdec ticks) tick-size)
       :qty BigDecimal/ZERO
       :bid? bid?})))

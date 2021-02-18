(ns io.sixtant.hum.arthur.level-diff
  "A change to an order book level.

  A level diff is either a [price qty] tuple to mean that the `price` level
  now has the new `qty`, or simply a `price` field to indicate a removal.

  Both numbers are represented as an integer number of ticks or lots according
  to the serialization context map, and have field lengths designated by the
  :pbits/:qbits attributes of the serialization context map.

  - A level diff

      +-------+-------+
      | Ticks |  Lots |
      | pbits | qbits |
      +-------+-------+
  - A level removal

      +-------+
      | Ticks |
      | pbits |
      +-------+

  The type of message (ask/bid and diff/removal) is interpreted from the
  message type flag in the surrounding message frame."
  (:require [io.sixtant.hum.utils :as u]
            [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.messages :as messages])
  (:import (java.io OutputStream InputStream NotSerializableException)))


(set! *warn-on-reflection* true)


(defn write-diff
  "Write a single {:price _ :qty _} level diff."
  [price qty context ^OutputStream out]
  (let [{:keys [pbits qbits tick-size lot-size]} context
        ticks (u/->ticks price tick-size)
        lots (u/->lots qty lot-size)]
    (try
      (b/encode (u/uints pbits qbits) out [ticks lots])

      (catch ArithmeticException e
        (if (re-matches
              #"Integer \d+ does not fit inside of \d+ bits."
              (ex-message e))
          (throw (NotSerializableException.))
          (throw e)))

      (catch IllegalArgumentException e
        (if (re-matches
              #"This sequence should have length \d+ but has really length \d+"
              (ex-message e))
          (throw (NotSerializableException.))
          (throw e))))))


(defn read-diff
  "Read a single {:price _ :qty _} level diff."
  [context bid? ^InputStream in]
  (let [{:keys [pbits qbits tick-size lot-size]} context
        [ticks lots] (b/decode (u/uints pbits qbits) in)]
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

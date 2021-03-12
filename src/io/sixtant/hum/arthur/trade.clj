(ns io.sixtant.hum.arthur.trade
  "A trade for some price and quantity.

  Both numbers are represented as an integer number of ticks or lots according
  to the serialization context map. The number of ticks and lots have field
  lengths designated by the :pbits / :qbits attributes of the serialization
  context map.

  The side of the trade is represented by a single bit: 1 if the maker was a
  bid, 0 if it was an ask.

  Since exchanges use different trade ID schemes, the trade ID is a
  variable-length series of bytes, and therefore comes last. If the trade id
  is numeric, the bit preceding the trade id is set, and the trade id is encoded
  as an unsigned long. Otherwise, it is a UTF-8 string.

      +-------+-------+------------+-------------+-----------------+
      | Ticks | Lots  | Maker Side | Numeric ID? |     Trade ID    |
      | pbits | qbits |   1 bit    |    1 bit    | Variable Length |
      +-------+-------+------------+-------------+-----------------+"
  (:require [io.sixtant.hum.utils :as u]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum.uint :as uint]
            [org.clojars.smee.binary.core :as b])
  (:import (java.io OutputStream InputStream NotSerializableException)))


(set! *warn-on-reflection* true)


(defn write-trade
  "Byte-serialize a single trade message."
  [{:keys [price qty maker-is-bid? tid]} context ^OutputStream out]
  (let [{:keys [pbits qbits tick-size lot-size]} context
        ticks (u/->ticks price tick-size)
        lots (u/->lots qty lot-size)
        [numeric? tid] (if (and (number? tid) (pos? tid))
                         [true (biginteger tid)]
                         [false (str tid)])
        numbers-codec (u/uints pbits qbits 1 1)]

    ;; Overflow
    (when (or (> (.bitLength ^BigInteger ticks) pbits)
              (> (.bitLength ^BigInteger lots) qbits))
      (throw (NotSerializableException.)))

    (let [trade-side-flag (if maker-is-bid? BigInteger/ONE BigInteger/ZERO)
          is-numeric-flag (if numeric? BigInteger/ONE BigInteger/ZERO)]
      (b/encode numbers-codec out [ticks lots trade-side-flag is-numeric-flag]))

    (if numeric?
      (.write out (byte-array (map b/ubyte->byte (uint/uint->ubytes tid))))
      (.write out (.getBytes (str tid) "UTF-8")))))


(defn read-trade
  "Deserialize a trade message from its byte representation."
  [context ^InputStream in]
  (let [{:keys [pbits qbits tick-size lot-size]} context

        numbers-codec (u/uints pbits qbits 1 1)
        [ticks lots trade-side-flag is-numeric-flag] (b/decode numbers-codec in)

        ;; The remaining bytes are the trade id
        id-bytes (.readAllBytes in)

        tid (if (= is-numeric-flag BigInteger/ONE)
              (uint/ubytes->uint (mapv b/byte->ubyte id-bytes))
              (String. id-bytes "UTF-8"))]
    (messages/map->Trade
      {:price (* (bigdec ticks) tick-size)
       :qty (* (bigdec lots) lot-size)
       :maker-is-bid? (= trade-side-flag BigInteger/ONE)
       :tid tid})))

(ns io.sixtant.hum.arthur.book-snapshot
  "A full order book snapshot message.

  Order books have a minimum price increment (tick) and minimum quantity
  increment (lot), so and this encoding of snapshots uses integer multiples of
  ticks and lots instead of decimal prices and quantities (as does the book diff
  encoding). Therefore, in addition to ask and bid levels, the snapshot contains
  the tick and lot sizes.

  Tick and lot sizes are each represented with one byte for the value and
  another for the scale. E.g. a tick size of 0.25 is represented as [25 2].


      +--------------+-----------+--------+------------+--------+-----------+-------------+--------------------+
      |  Price Bits  | Qty Bits  |  Tick  | Tick Scale |  Lot   | Lot Scale | # of Levels |        Levels      |
      |    1 byte    |  1 byte   | 1 byte |    1 byte  | 1 byte |   1 byte  |   2 bytes   |  (Variable Length) |
      +--------------+-----------+--------+------------+--------+-----------+-------------+--------------------+

  The snapshot additionally contains information about the field sizes for tick
  and lot sizes. This allows the encoding to be relatively aggressive about
  shrinking the field sizes, e.g. if the price (number of ticks) can be
  represented with 13 bits, and the quantity (number of lots) can be represented
  with 21 bits, together the price and quantity can be represented with just 4
  bytes. If an overflow occurs, a new snapshot with updated field sizes is
  simply written before continuing.

  Finally, after the four tick and lot fields, two bytes specify the number
  of book levels which follow. Each book level contains the integer number of
  ticks representing the price, a single bit representing book side (1 for bid,
  0 for ask), and an integer number of lots representing the quantity.

      +------------+-------+----------+
      |    Ticks   |  Side |   Lots   |
      | Price Bits | 1 bit | Qty Bits |
      +------------+-------+----------+"
  (:require [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.utils :as u]
            [io.sixtant.hum.messages :as messages])
  (:import (java.io OutputStream InputStream)))


(set! *warn-on-reflection* true)


(def header-codec ; codec for all fields except the levels
  (b/ordered-map
    :pbits            :byte
    :qbits            :byte
    :tick             :byte
    :tick-scale       :byte
    :lot              :byte
    :lot-scale        :byte
    :nlevels          :ushort))


(defn- write-levels [levels side-bit context ^OutputStream out]
  (let [{:keys [pbits qbits tick-size lot-size]} context
        codec (u/uints pbits 1 qbits)]
    (doseq [{:keys [price qty] :as lvl} levels]
      ;; Represent price + qty as integer multiples of tick / lot size
      (try
        (let [ticks (u/->ticks price tick-size)
              lots (u/->lots qty lot-size)]
          (b/encode codec out [ticks side-bit lots]))
        (catch ArithmeticException _
          (let [data {:context context :level lvl}]
            (throw (ex-info "Level tick or lot is too large" data))))))))


(defn write-snapshot
  "Write a messages/OrderBookSnapshot to `out` and return a _serialization
  context_ including field size information used to read and write diff
  messages."
  [snapshot ctx ^OutputStream out]
  (let [{:keys [^BigDecimal tick-size ^BigDecimal lot-size bids asks]}
        snapshot]
    (let [;; Represent tick / lot sizes with integers
          [tick tick-scale] (u/dec-as-ints tick-size)
          [lot lot-scale] (u/dec-as-ints lot-size)

          ;; The serialized message, without the levels
          header {:pbits (u/max-price-bits tick-size asks)
                  ; allow for 2x the max qty to occur
                  :qbits (inc (u/max-qty-bits lot-size (concat asks bids)))
                  :tick tick
                  :tick-scale tick-scale
                  :lot lot
                  :lot-scale lot-scale
                  :nlevels (+ (count bids) (count asks))}

          ;; The serialization context is the header plus a couple fields
          context (assoc header :tick-size tick-size :lot-size lot-size)]

      (b/encode header-codec out header)
      (write-levels bids BigInteger/ONE context out)
      (write-levels asks BigInteger/ZERO context out)

      (merge ctx context))))


(defn read-snapshot
  "Deserialize the order book snapshot bytes, and return a tuple of
  `[snapshot, serialization context]`."
  [^InputStream in]
  ; read all fixed-length fields
  (let [{:keys [pbits qbits tick tick-scale lot lot-scale nlevels] :as header}
        (b/decode header-codec in)

        tick-size (u/ints-as-dec [tick tick-scale])
        lot-size (u/ints-as-dec [lot lot-scale])

        level-codec (u/uints pbits 1 qbits)]
    [(reduce
       (fn [x _]
         (let [[ticks bid? lots] (b/decode level-codec in)
               level {:price (* ticks tick-size)
                      :qty (* lots lot-size)}
               side (if (= bid? BigInteger/ONE) :bids :asks)]
           (update x side conj level)))
       (messages/->OrderBookSnapshot [] [] nil tick-size lot-size)
       (range nlevels))
     (assoc header :tick-size tick-size :lot-size lot-size)]))

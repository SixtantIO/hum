(ns io.sixtant.hum.utils
  (:require [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.uint :as uint])
  (:import (java.math RoundingMode)))


(set! *warn-on-reflection* true)


(defn pow2 "Integer power of 2" [n] (bigint (Math/pow 2 n)))


(defn uints
  "A codec for packing a vector of unsigned integers of arbitrary bit lengths
  into the smallest possible unsigned bytes vector.

  E.g. (uints-codec 3 5) packs a 3-bit integer and a 5-bit integer into a
  single unsigned byte.

  Checking for overflow is the responsibility of the caller."
  [& bit-lengths]
  (let [n (count bit-lengths)
        bit-length (reduce + bit-lengths)
        byte-length (long (Math/ceil (/ bit-length 8.0)))]
    (b/compile-codec
      (b/repeated :ubyte :length byte-length)
      (fn [uints]
        (assert (= (count uints) n) (str "codec is for " n " integers"))
        (let [rpad-zeros (fn [bs l]
                           (if (< (count bs) l)
                             (into bs (repeat (- l (count bs)) 0x00))
                             bs))]
          (-> uints
              (uint/pack-ints bit-lengths)
              uint/uint->ubytes
              (rpad-zeros byte-length))))
      (fn [ubytes]
        (-> ubytes uint/ubytes->uint (uint/unpack-ints bit-lengths))))))


(defn ->ticks
  "Convert a price to an integer number of ticks."
  [price tick-size]
  (try
    (.toBigIntegerExact (bigdec (/ price tick-size)))
    (catch ArithmeticException e
      (throw
        (IllegalArgumentException.
          (format "Couldn't encode price %s with tick size %s" price tick-size))))))


(defn ->lots
  "Convert a qty to an integer number of lots."
  [qty lot-size]
  (try
    (.toBigIntegerExact (bigdec (/ qty lot-size)))
    (catch ArithmeticException e
      (throw
        (IllegalArgumentException.
          (format "Couldn't encode qty %s with lot size %s" qty lot-size))))))


(defn max-price-bits
  "The number of bits necessary to represent the largest price in the book in
  integer ticks."
  [^BigDecimal tick-size asks]
  (let [^BigDecimal max-price (transduce (map :price) (completing max) 0 asks)]
    (try
      (-> (bigdec (/ max-price tick-size))
          (.setScale 0 RoundingMode/UNNECESSARY)
          (.toBigInteger)
          (.bitLength))
      (catch ArithmeticException e
        (throw
          (ex-info
            "Tick size incorrect."
            {:tick tick-size :price max-price}))))))


(defn max-qty-bits
  "The number of bits necessary to represent the largest quantity in the book
  in integer lots."
  [^BigDecimal lot-size levels]
  (let [^BigDecimal max-qty (transduce (map :qty) (completing max) 0 levels)]
    (try
      (-> (bigdec (/ max-qty lot-size))
          (.setScale 0 RoundingMode/UNNECESSARY)
          (.toBigInteger)
          (.bitLength))
      (catch ArithmeticException e
        (throw
          (ex-info
            "Lot size incorrect."
            {:tick lot-size :qty max-qty}))))))


(defn dec-as-ints
  "Represent the decimal `d` as a tuple of integers, `[value, scale]`."
  [^BigDecimal d]
  (let [scale (-> d .stripTrailingZeros .scale)]
    [(-> d (.scaleByPowerOfTen scale) .intValueExact)
     scale]))


(defn ints-as-dec
  "Build a decimal from a tuple of integers, `[value, scale]`."
  [[value scale]]
  (.scaleByPowerOfTen (bigdec value) (- scale)))

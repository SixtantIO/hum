(ns io.sixtant.hum.codec1
  "First stab at a codec ..."
  (:require [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.serialize :as ser]
            [io.sixtant.hum.messages :as messages]
            [clojure.set :as set]
            [taoensso.truss :as truss])
  (:import (java.math RoundingMode)
           (java.io ByteArrayOutputStream ByteArrayInputStream OutputStream InputStream NotSerializableException Closeable EOFException)
           (io.sixtant.hum.messages OrderBookSnapshot OrderBookDiff)
           (clojure.lang IFn)))


(set! *warn-on-reflection* true)


;;; Helpers


(defn ->ticks [price tick-size]
  (try
    (.toBigIntegerExact (bigdec (/ price tick-size)))
    (catch ArithmeticException e
      (throw
        (IllegalArgumentException.
          (format "Couldn't encode price %s with tick size %s" price tick-size))))))


(defn ->lots [qty lot-size]
  (try
    (.toBigIntegerExact (bigdec (/ qty lot-size)))
    (catch ArithmeticException e
      (throw
        (IllegalArgumentException.
          (format "Couldn't encode qty %s with lot size %s" qty lot-size))))))


(defn biginteger-to-padded-bytes [^BigInteger i byte-length]
  (let [unpadded (.toByteArray i)
        padding (- byte-length (count unpadded))]

    (when (neg? padding)
      (throw
        (ArithmeticException.
          (format "%s cannot be represented in %s bytes" i byte-length))))

    (if (pos? padding)
      (let [padding-byte (if (neg? i) 0xFF 0x00)]
        (byte-array (concat (repeat padding padding-byte) unpadded)))
      unpadded)))


;;; DTOs -- low-level messages whose fields map 1:1 with their binary
;;; representation


(defrecord Level [ticks  ; biginteger
                  lots]) ; biginteger (signed according to book side)


(def ^:private snapshot-codec ; all fields except the levels
  [:ulong :byte :byte :byte :byte :byte :byte :ushort])


(def snapshot-codec ; codec for all fields except the levels
  (b/ordered-map
    :timestamp  :ulong ; microsecond unix epoch timestamp
    :pbytes     :byte
    :qbytes     :byte
    :tick       :byte
    :tick-scale :byte
    :lot        :byte
    :lot-scale  :byte
    :nlevels    :ushort))


(defrecord Snapshot [timestamp
                     pbytes
                     qbytes
                     tick
                     tick-scale
                     lot
                     lot-scale
                     nlevels
                     levels]
  ser/DTO
  (serialize [this out]
    (assert (< nlevels 65535))
    (b/encode snapshot-codec out this) ; write all fixed-length fields
    (try
      (doseq [level levels]
        (.write ^OutputStream out ^bytes (biginteger-to-padded-bytes (:ticks level) pbytes))
        (.write ^OutputStream out ^bytes (biginteger-to-padded-bytes (:lots level) qbytes)))
      (catch ArithmeticException _
        (throw (ex-info "Level tick or lot is too large" this)))))

  (deserialize [this in]
    ; read all fixed-length fields
    (let [{:keys [pbytes qbytes nlevels] :as this}
          (into this (b/decode snapshot-codec in))

          levels (mapv
                   (fn [_]
                     (let [ticks (BigInteger. (.readNBytes ^InputStream in pbytes))
                           lots (BigInteger. (.readNBytes ^InputStream in qbytes))]
                       (->Level ticks lots)))
                   (range nlevels))]
      (assoc this :levels levels))))


;; Context is created from a snapshot & is necessary to interpret diff messages
;; because it specifies field lengths & a base timestamp, from which we count
;; using a more compact number.
(defrecord Context [timestamp pbytes qbytes tick-size lot-size])


(defrecord Diff [timestamp-offset ; uint -- microseconds since snapshot
                 bid?             ; byte -- 0 if ask, 1 if bid
                 ticks            ; bytes[pbytes] (biginteger)
                 lots             ; bytes[qbytes] (biginteger)
                 context]
  ser/DTO
  (serialize [this out]
    (b/encode :uint out timestamp-offset)
    (b/encode :byte out (if bid? 1 0))
    (.write ^OutputStream out ^bytes (biginteger-to-padded-bytes ticks (:pbytes context)))
    (.write ^OutputStream out ^bytes (biginteger-to-padded-bytes lots (:qbytes context))))

  (deserialize [this in]
    (let [timestamp-offset (bigint (b/decode :uint in))
          bid? (if (pos? (b/decode :byte in)) true false)
          ticks (BigInteger. (.readNBytes ^InputStream in (:pbytes context)))
          lots (BigInteger. (.readNBytes ^InputStream in (:qbytes context)))]
      (assoc this
        :timestamp-offset timestamp-offset
        :bid? bid?
        :ticks ticks
        :lots lots))))


;;; Message construction


(defmulti message->dto
  "Construct a binary-serializable DTO from an order book message."
  (fn [message context] (type message)))


(defmulti dto->message
  "Reconstruct an order book message from a binary-serializable DTO."
  (fn [dto context] (type dto)))


(defn- max-price-bytes [^BigDecimal tick-size asks]
  (let [^BigDecimal max-price (transduce (map :price) (completing max) 0 asks)]
    (try
      (-> (bigdec (/ max-price tick-size))
          (.setScale 0 RoundingMode/UNNECESSARY)
          (.toBigInteger)
          (.toByteArray)
          (count))
      (catch ArithmeticException e
        (throw
          (ex-info
            "Tick size incorrect."
            {:tick tick-size :price max-price}))))))


(defn- max-qty-bytes [^BigDecimal lot-size levels]
  (let [^BigDecimal max-qty (transduce (map :qty) (completing max) 0 levels)]
    (try
      (-> (bigdec (/ max-qty lot-size))
          (.setScale 0 RoundingMode/UNNECESSARY)
          (.toBigInteger)
          (.toByteArray)
          (count))
      (catch ArithmeticException e
        (throw
          (ex-info
            "Lot size incorrect."
            {:tick lot-size :qty max-qty}))))))


(defmethod message->dto OrderBookSnapshot
  [{:keys [^BigDecimal tick-size ^BigDecimal lot-size bids asks timestamp]} _]
  (let [tick-scale (-> tick-size .stripTrailingZeros .scale)
        tick (-> tick-size (.scaleByPowerOfTen tick-scale) .intValueExact)

        lot-scale (-> lot-size .stripTrailingZeros .scale)
        lot (-> lot-size (.scaleByPowerOfTen lot-scale) .intValueExact)

        bid-levels (mapv
                     (fn [{:keys [price qty]}]
                       (->Level (->ticks price tick-size) (->lots qty lot-size)))
                     bids)
        ask-levels (mapv
                     (fn [{:keys [price qty]}]
                       (->Level (->ticks price tick-size) (->lots (- qty) lot-size)))
                     asks)
        levels (into bid-levels ask-levels)]
    (map->Snapshot
      {:timestamp (bigint timestamp)
       :pbytes (max-price-bytes tick-size asks)
       :qbytes (max-qty-bytes lot-size (concat asks bids))
       :tick tick
       :tick-scale tick-scale
       :lot lot
       :lot-scale lot-scale
       :nlevels (count levels)
       :levels levels})))


(defmethod dto->message Snapshot [dto context]
  (let [{:keys [timestamp tick tick-scale lot lot-scale levels]}
        dto

        tick-size (.scaleByPowerOfTen (bigdec tick) (- tick-scale))
        lot-size (.scaleByPowerOfTen (bigdec lot) (- lot-scale))
        parse-level (fn [{:keys [ticks lots]}]
                      {:price (* (bigdec ticks) tick-size)
                       :qty (.abs ^BigDecimal (* (bigdec lots) lot-size))})

        {:keys [bids asks]} (reduce
                              (fn [levels level]
                                (update
                                  levels
                                  (if (pos? (:lots level)) :bids :asks)
                                  conj
                                  (parse-level level)))
                              {:bids [] :asks []}
                              levels)]
    (messages/order-book-snapshot
      {:bids bids
       :asks asks
       :timestamp timestamp
       :tick-size tick-size
       :lot-size lot-size})))


(defn >context
  "Contextualize writes of diffs in relation to the most recent snapshot."
  [snapshot-dto]
  (let [{:keys [tick-size lot-size]} (dto->message snapshot-dto nil)]
    (map->Context
      {:timestamp (:timestamp snapshot-dto)
       :pbytes (:pbytes snapshot-dto)
       :qbytes (:qbytes snapshot-dto)
       :tick-size tick-size
       :lot-size lot-size})))


(def max-ts-offset (* Integer/MAX_VALUE 2))


(defmethod message->dto OrderBookDiff
  [{:keys [price qty bid? timestamp]}
   {:keys [tick-size lot-size] :as ctx}]
  (assert ctx "Book diff is contextualized by a snapshot.")
  (let [ts-offset (bigint (- timestamp (:timestamp ctx)))]
    (assert (>= ts-offset 0))
    (when-not (< ts-offset max-ts-offset)
      (throw (NotSerializableException. "Timestamp offset overflow")))
    (map->Diff
      {:timestamp-offset ts-offset
       :bid? bid?
       :ticks (biginteger (->ticks price tick-size))
       :lots (biginteger (->lots qty lot-size))
       :context ctx})))


(defmethod dto->message Diff
  [{:keys [timestamp-offset bid? ticks lots context]}
   {:keys [tick-size lot-size timestamp] :as ctx}]
  (messages/order-book-diff
    {:price (* (bigdec ticks) tick-size)
     :qty (* (bigdec lots) lot-size)
     :bid? bid?
     :timestamp (+ timestamp timestamp-offset)}))


(defrecord MessageFrame [message-type-flag message-byte-length message-bytes]
  ser/DTO
  (serialize [this out]
    (b/encode [:byte :uint] out [message-type-flag message-byte-length])
    (.write ^OutputStream out ^bytes message-bytes))

  (deserialize [this in]
    (let [[tf bl] (b/decode [:byte :uint] in)]
      (assoc this
        :message-type-flag tf
        :message-byte-length bl
        :message-bytes (.readNBytes ^InputStream in bl)))))


(def type->flag {Snapshot 0 Diff 1})
(def flag->type (set/map-invert type->flag))
(def dto-instance {Snapshot (map->Snapshot {}) Diff (map->Diff {})})


(def max-message-bytes (* 2 Integer/MAX_VALUE)) ; bc length is a :uint


(defn assert-bijective! [dto dto-bytes]
  (let [deserialized (ser/deserialize dto (ByteArrayInputStream. dto-bytes))]
    (when-not (= deserialized dto)
      (throw (ex-info "Serialization not bijective!"
                      {:message dto
                       :deserialized deserialized})))))


(defn frame
  "Construct a message, itself serializable, from a DTO."
  [dto]
  (truss/have type->flag (type dto))
  (let [type-flag (type->flag (type dto))
        dto-bytes (ByteArrayOutputStream.)
        _ (ser/serialize dto dto-bytes)
        dto-bytes (.toByteArray dto-bytes)]
    (truss/have #(<= % max-message-bytes) (count dto-bytes))
    (assert-bijective! dto dto-bytes)
    (->MessageFrame type-flag (count dto-bytes) dto-bytes)))


(defn read-frame
  "Read the next message frame from the InputStream, or nil if EOF."
  [^InputStream in]
  (try
    (ser/deserialize (map->MessageFrame {}) in)
    (catch EOFException _
      nil)))


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
          dto (try
                (message->dto message ctx)
                (catch NotSerializableException _
                  (let [snap (some-> message :snapshot-delay deref)]
                    (assert snap (str "When a diff can't be serialized, it "
                                      "must include a :snapshot-delay so that "
                                      "a fresh snapshot can be written."))
                    (message->dto snap nil))))
          fr (frame dto)]

      ;; Every snapshot creates a fresh context, with field lengths etc.
      (when (= (flag->type (:message-type-flag fr)) Snapshot)
        (vreset! context (>context dto)))

      (ser/serialize fr out)
      true)))


(defrecord L2Reader [^InputStream in context]
  Closeable
  (close [this]
    (vreset! context ::closed)
    (.close in))

  IFn
  (invoke [this]
    (let [ctx @context]
      (assert (not= ctx ::closed) "Reader is open.")
      (when-let [frame (read-frame in)]
        (let [dto (-> (:message-type-flag frame)
                      flag->type
                      dto-instance
                      (assoc :context ctx))
              dto (ser/deserialize dto (ByteArrayInputStream. (:message-bytes frame)))]

          ;; Every snapshot creates a fresh context, with field lengths etc.
          (when (= (flag->type (:message-type-flag frame)) Snapshot)
            (vreset! context (>context dto)))

          (dto->message dto ctx))))))


(comment
  (require 'criterium.core)

  (def micro-timestamp (* (System/currentTimeMillis) 1000))

  (def snap
    (messages/order-book-snapshot
      {:tick-size 0.01M
       :lot-size 0.0001M
       :bids [{:price 100.52M :qty 1.2345M}]
       :asks [{:price 102.52M :qty 5.2345M}]
       :timestamp micro-timestamp}))

  (def serialized
    (ser/with-bytes
      out
      (ser/serialize (message->dto snap nil) out)))

  (def deserialized
    (-> (map->Snapshot {})
        (ser/deserialize (ByteArrayInputStream. (byte-array serialized)))
        (dto->message nil)))

  (= deserialized snap)

  (def ctx
    (-> snap (message->dto nil) (context)))

  (def diff
    (messages/order-book-diff
      {:price 102.52M
       :qty 2.35M
       :bid? true
       :timestamp (+ micro-timestamp 100)}))

  (def serialized-diff
    (ser/with-bytes out (ser/serialize (message->dto diff ctx) out)))

  (def deserialized-diff
    (-> (map->Diff {:context ctx})
        (ser/deserialize (ByteArrayInputStream. (byte-array serialized-diff)))
        (dto->message ctx)))

  (= deserialized-diff diff)

  )

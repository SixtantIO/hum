(ns io.sixtant.hum.codec2
  (:require [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.serialize :as ser]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum.uint :as uint]
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


(defn uints
  "A codec for packing a vector of unsigned integers of arbitrary bit lengths
  into the smallest possible unsigned bytes vector.

  E.g. (uints-codec 3 5) packs a 3-bit integer and a 5-bit integer into a
  single unsigned byte."
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


;;; The reader/writer binds these vars sequentially based on the DTOs that have
;;; been written.


; the most recent full millisecond timestamp, against which DTO ts offsets are summed
(def ^:dynamic *timestamp* nil)
(def ^:dynamic *diff-codec* nil) ; changes as the price+qty field bit lengths change
(def ^:dynamic *tick-size* nil) ; for converting integer ticks to prices
(def ^:dynamic *lot-size* nil) ; for converting integer lots to qtys


;;; DTOs -- low-level messages whose fields map 1:1 with their binary
;;; representation, represented by 3-bit integers.


(def ^:const SNAPSHOT  0)
(def ^:const TIMESTAMP 1)
(def ^:const DIFF      2)


(defprotocol DTOTyped (get-type-flag [this]))


(def snapshot-codec ; codec for all fields except the levels
  (b/ordered-map
    :pbits            :byte
    :qbits            :byte
    :tick             :byte
    :tick-scale       :byte
    :lot              :byte
    :lot-scale        :byte
    :nlevels          :ushort))


(defrecord Snapshot [pbits
                     qbits
                     tick
                     tick-scale
                     lot
                     lot-scale
                     nlevels
                     levels] ; [(biginteger ticks) (biginteger lots)]
  ser/DTO
  (serialize [this out]
    (assert (< nlevels 65535))
    (b/encode snapshot-codec out this) ; write all fixed-length fields
    (try
      (let [codec (uints pbits 1 (dec qbits))]
        (doseq [[ticks lots] levels]
          (b/encode
            codec
            out
            [ticks
             (if (pos? lots) BigInteger/ONE BigInteger/ZERO)
             (.abs ^BigInteger lots)])))
      (catch ArithmeticException _
        (throw (ex-info "Level tick or lot is too large" this)))))

  (deserialize [this in]
    ; read all fixed-length fields
    (let [{:keys [pbits qbits nlevels] :as this}
          (into this (b/decode snapshot-codec in))

          codec (uints pbits 1 (dec qbits))
          levels (mapv
                   (fn [_]
                     (let [[ticks bid? lots]
                           (b/decode codec in)]
                       [ticks ((if (pos? bid?) + -) lots)]))
                   (range nlevels))]
      (assoc this :levels levels))))


;; Timestamps are recorded in the message frame, this DTO is a special entry
;; that needs to happen approximately once per minute, and allows reducing each
;; other row's size by two bytes by storing only a timestamp offset.
(defrecord Timestamp [timestamp] ; millisecond unix epoch timestamp
  ser/DTO
  (serialize [this out] (b/encode :ulong out timestamp))
  (deserialize [this in] (assoc this :timestamp (b/decode :ulong in))))


(defn ticks+side+lots-codec [pbits qbits]
  (uints
    pbits ;; First bits are the # of ticks (price)
    1 ;; Next, single bit is the side (1 = bid, 0 = ask)
    (dec qbits))) ;; Finally, the last bits are # of lots (qty)


(defn >diff-codec [snapshot-dto]
  (ticks+side+lots-codec (:pbits snapshot-dto) (:qbits snapshot-dto)))


(defrecord Diff [bid?             ; first bit of lots; 0 if ask, 1 if bid
                 ticks            ; bytes[pbits/8] (biginteger)
                 lots]            ; bytes[qbits/8] (biginteger)

  ser/DTO
  (serialize [this out]
    (b/encode *diff-codec* out [ticks bid? lots]))

  (deserialize [this in]
    (let [[ticks bid? lots] (b/decode *diff-codec* in)]
      (assoc this
        :bid? bid?
        :ticks ticks
        :lots lots))))


(extend-protocol DTOTyped
  ;; The DTOs from this namespace
  Snapshot (get-type-flag [this] SNAPSHOT)
  Diff (get-type-flag [this] DIFF)
  Timestamp (get-type-flag [this] TIMESTAMP)

  ;; The corresponding message records
  OrderBookSnapshot (get-type-flag [this] SNAPSHOT)
  OrderBookDiff (get-type-flag [this] DIFF))


;;; Message construction


(defmulti message->dto
  "Construct a binary-serializable DTO from an order book message."
  (fn [message] (get-type-flag message)))


(defmulti dto->message
  "Reconstruct an order book message from a binary-serializable DTO."
  (fn [dto] (get-type-flag dto)))


(defn- max-price-bits [^BigDecimal tick-size asks]
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


(defn- max-qty-bits [^BigDecimal lot-size levels]
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


(defmethod message->dto SNAPSHOT
  [{:keys [^BigDecimal tick-size ^BigDecimal lot-size bids asks timestamp]}]
  (let [tick-scale (-> tick-size .stripTrailingZeros .scale)
        tick (-> tick-size (.scaleByPowerOfTen tick-scale) .intValueExact)

        lot-scale (-> lot-size .stripTrailingZeros .scale)
        lot (-> lot-size (.scaleByPowerOfTen lot-scale) .intValueExact)

        bid-levels (mapv
                     (fn [{:keys [price qty]}]
                       [(->ticks price tick-size) (->lots qty lot-size)])
                     bids)
        ask-levels (mapv
                     (fn [{:keys [price qty]}]
                       [(->ticks price tick-size) (->lots (- qty) lot-size)])
                     asks)
        levels (into bid-levels ask-levels)]
    (map->Snapshot
      {; allocate enough bits for the maximum price currently on the book
       :pbits (max-price-bits tick-size asks)
       ; allow for 2x the qty, plus one extra bit for the side of the order
       :qbits (+ (max-qty-bits lot-size (concat asks bids)) 2)
       :tick tick
       :tick-scale tick-scale
       :lot lot
       :lot-scale lot-scale
       :nlevels (count levels)
       :levels levels})))


(defmethod dto->message SNAPSHOT [dto]
  (let [{:keys [tick tick-scale lot lot-scale levels]}
        dto

        tick-size (.scaleByPowerOfTen (bigdec tick) (- tick-scale))
        lot-size (.scaleByPowerOfTen (bigdec lot) (- lot-scale))
        parse-level (fn [ticks lots]
                      {:price (* (bigdec ticks) tick-size)
                       :qty (.abs ^BigDecimal (* (bigdec lots) lot-size))})

        {:keys [bids asks]} (reduce
                              (fn [levels [ticks lots]]
                                (update
                                  levels
                                  (if (pos? lots) :bids :asks)
                                  conj
                                  (parse-level ticks lots)))
                              {:bids [] :asks []}
                              levels)]
    (messages/order-book-snapshot
      {:bids bids
       :asks asks
       :tick-size tick-size
       :lot-size lot-size})))


(defmethod message->dto DIFF
  [{:keys [price qty bid?]}]
  (map->Diff
    {:bid? (if bid? BigInteger/ONE BigInteger/ZERO)
     :ticks (biginteger (->ticks price *tick-size*))
     :lots (biginteger (->lots qty *lot-size*))}))


(defmethod dto->message DIFF
  [{:keys [bid? ticks lots]}]
  (messages/order-book-diff
    {:price (* (bigdec ticks) *tick-size*)
     :qty (* (bigdec lots) *lot-size*)
     :bid? (pos? bid?)}))


(defrecord MessageFrame
  [message-type-flag message-byte-length timestamp-offset message-bytes]
  ser/DTO
  (serialize [this out]
    ;; The first byte is split: first 3 bits are msg type, next 5 are msg
    ;; length. If the message length doesn't fit, write '0' for the next 5 bits
    ;; and follow with a uint.
    (b/encode (uints 3 5) out [(biginteger message-type-flag)
                               (if (< message-byte-length 32)
                                 (biginteger message-byte-length)
                                 BigInteger/ZERO)])

    (when-not (< message-byte-length 32)
      (b/encode :uint out message-byte-length))

    (b/encode :ushort out timestamp-offset)

    (.write ^OutputStream out ^bytes message-bytes))

  (deserialize [this in]
    (let [[tf ml] (b/decode (uints 3 5) in)
          ml (if (zero? ml) (b/decode :uint in) ml)
          timestamp-offset (b/decode :ushort in)]
      (assoc this
        :message-type-flag tf
        :message-byte-length ml
        :timestamp-offset timestamp-offset
        :message-bytes (.readNBytes ^InputStream in ml)))))


(def ^:const max-message-bytes (dec (long (Math/pow 2 32)))) ; bc length is a :uint
(def ^:const max-ts-offset (dec (long (Math/pow 2 16))))


(defn frame
  "Construct a message, itself serializable, from a DTO."
  [dto ts-offset]
  (let [dto-bytes (ByteArrayOutputStream.)
        _ (ser/serialize dto dto-bytes)
        dto-bytes (.toByteArray dto-bytes)]
    (truss/have #(<= 1 % max-message-bytes) (count dto-bytes))
    (truss/have #(<= % max-ts-offset) ts-offset)
    (->MessageFrame (get-type-flag dto) (count dto-bytes) ts-offset dto-bytes)))


(defn read-frame
  "Read the next message frame from the InputStream, or nil if EOF."
  [^InputStream in]
  (try
    (ser/deserialize (map->MessageFrame {}) in)
    (catch EOFException _
      nil)))


(defrecord L2Writer [^OutputStream out tick-size lot-size diff-codec timestamp]
  Closeable
  (close [this]
    (.flush out)
    (.close out))


  IFn
  (invoke [this message]
    ;; First, check difference between message ts and last written Timestamp
    ;; frame. If it's too large for a short, write a new Timestamp frame.
    (let [msg-ts (:timestamp message)
          ts @timestamp
          ts (if (> (- msg-ts ts) max-ts-offset)
               (do
                 (ser/serialize (frame (->Timestamp (bigint msg-ts)) 0) out)
                 (vreset! timestamp msg-ts))
               ts)]
      (binding [*tick-size* @tick-size
                *lot-size* @lot-size
                *diff-codec* @diff-codec
                *timestamp* ts]
        (let [msg-ts (:timestamp message)
              [dto fr]
              (try
                (let [dto (message->dto message)]
                  [dto (frame dto (- msg-ts ts))])
                (catch NotSerializableException _
                  (let [snap (some-> message :snapshot-delay deref)]
                    (assert snap (str "When a diff can't be serialized, it "
                                      "must include a :snapshot-delay so that "
                                      "a fresh snapshot can be written."))
                    (let [dto (message->dto snap)]
                      [dto (frame dto (- msg-ts ts))]))))]

          ;; Every snapshot creates a fresh context, with field lengths etc.
          (when (= (int (get-type-flag dto)) SNAPSHOT)
            (vreset! tick-size (:tick-size message))
            (vreset! lot-size (:lot-size message))
            (vreset! diff-codec (>diff-codec dto)))

          ;; Finally, write the frame
          (ser/serialize fr out)

          true)))))


(defrecord L2Reader [^InputStream in tick-size lot-size diff-codec timestamp]
  Closeable
  (close [this]
    (.close in))

  IFn
  (invoke [this]
    (when-let [frame (read-frame in)]
      (binding [*tick-size* @tick-size
                *lot-size* @lot-size
                *diff-codec* @diff-codec
                *timestamp* @timestamp]
        (let [dto (-> (condp = (:message-type-flag frame)
                        DIFF (map->Diff {})
                        TIMESTAMP (map->Timestamp {})
                        SNAPSHOT (map->Snapshot {}))
                      (ser/deserialize
                        (ByteArrayInputStream. (:message-bytes frame))))]

          (condp = (:message-type-flag frame)
            DIFF
            (let [message (dto->message dto)]
              (assoc message :timestamp (+ *timestamp* (:timestamp-offset frame))))

            ;; Reset the reference timestamp
            TIMESTAMP
            (do
              (vreset! timestamp (:timestamp dto))
              (this)) ; doesn't return a message, so read the next frame

            ;; Every snapshot creates a fresh context, with field lengths etc.
            SNAPSHOT
            (let [message (dto->message dto)]
              (vreset! tick-size (:tick-size message))
              (vreset! lot-size (:lot-size message))
              (vreset! diff-codec (>diff-codec dto))
              (assoc message :timestamp (+ *timestamp* (:timestamp-offset frame))))))))))

(defn writer [out]
  (->L2Writer out (volatile! nil) (volatile! nil) (volatile! nil) (volatile! 0)))

(defn reader [in]
  (->L2Reader in (volatile! nil) (volatile! nil) (volatile! nil) (volatile! 0)))

(comment
  (require 'criterium.core)
  (ns-unmap *ns* 'message->dto)
  (ns-unmap *ns* 'dto->message)

  (def timestamp (System/currentTimeMillis))

  (def snap
    (messages/order-book-snapshot
      {:tick-size 0.01M
       :lot-size 0.0001M
       :bids [{:price 100.52M :qty 1.2345M}]
       :asks [{:price 102.52M :qty 5.2345M}]}))

  (def serialized
    (ser/with-bytes out (ser/serialize (message->dto snap) out)))

  (def deserialized
    (binding [*tick-size* 0.01M
              *lot-size* 0.0001M]
      (-> (map->Snapshot {})
          (ser/deserialize (ByteArrayInputStream. (byte-array serialized)))
          (dto->message))))

  (= deserialized snap)

  (def ctx
    (-> snap (message->dto) (>context)))

  (def diff
    (messages/order-book-diff
      {:price 102.52M
       :qty 2.35M
       :bid? true}))

  (def serialized-diff
    (binding [*tick-size* 0.01M
              *lot-size* 0.0001M
              *diff-codec* (>diff-codec (message->dto snap))]
      (ser/with-bytes out (ser/serialize (message->dto diff) out))))

  (def deserialized-diff
    (binding [*tick-size* 0.01M
              *lot-size* 0.0001M
              *diff-codec* (>diff-codec (message->dto snap))]
      (-> (map->Diff {})
          (ser/deserialize (ByteArrayInputStream. (byte-array serialized-diff)))
          (dto->message))))

  (= deserialized-diff diff)

  )

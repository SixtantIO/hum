(ns io.sixtant.hum.messages
  "High-level messages to serialize / read.")


(set! *warn-on-reflection* true)


(defrecord OrderBookSnapshot [bids asks timestamp tick-size lot-size])


(defn- exact-multiple? [^BigDecimal n ^BigDecimal quantum]
  (try
    (.toBigIntegerExact ^BigDecimal (/ n quantum))
    true
    (catch ArithmeticException _
      false)))


(defn- denominator* [^BigDecimal quantum numbers]
  (if-let [n (first numbers)]
    (if (exact-multiple? n quantum)
      (recur quantum (rest numbers))
      (recur (/ quantum 10M) numbers))
    quantum))


(defn adjust-tick-size
  "Adjust the `tick-size` downward by powers of ten, if necessary, to match the
  given order book levels shaped [{:price <bigdec>}]."
  [tick-size book-levels]
  (denominator* tick-size (map :price book-levels)))


(defn adjust-lot-size
  "Adjust the `lot-size` downward by powers of ten, if necessary, to match the
  given order book levels shaped [{:qty <bigdec>}]."
  [lot-size book-levels]
  (denominator* lot-size (map :qty book-levels)))


(defn order-book-snapshot
  "A L2 order book message.

  The `bids` and `asks` are series of {:price <bigdec>, :qty <bigdec>} levels,
  and tick/lot size are the minimum price and quantity increments for the order
  book."
  [{:keys [bids asks timestamp tick-size lot-size] :as data}]
  (map->OrderBookSnapshot data))


(defrecord OrderBookDiff [price qty bid? timestamp snapshot-delay])


(defn order-book-diff
  "An L2 order book diff, with a BigDecimal price level and new quantity (zero
  if the level has been removed).

  :snapshot-delay, if included, dereferences to an order book snapshot message.
  Include for codecs which might need to write a new snapshot at any time."
  [{:keys [price qty bid? timestamp snapshot-delay] :as data}]
  (map->OrderBookDiff data))

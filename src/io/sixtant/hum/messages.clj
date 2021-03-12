(ns io.sixtant.hum.messages
  "High-level messages which can be processed by codecs.")


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

  :snapshot-delay, if included, dereferences to an order book snapshot
  message. See the README or book_snapshot.clj for more information about why
  this is the case."
  [{:keys [price qty bid? timestamp snapshot-delay] :as data}]
  (map->OrderBookDiff data))


(defrecord Trade [price qty maker-is-bid? tid timestamp snapshot-delay])


(defn trade
  "A trade with BigDecimal price and quantity fields. In the interest of
  compression, the maker and taker order ids are not stored, just the
  trade id.

  The if the exchange uses a numeric trade id scheme with positive, unsigned
  integers, pass in the trade id as a number, and it will be written with
  higher compression. Otherwise, trade ids are written as UTF-8 strings.

  :snapshot-delay, if included, dereferences to an order book snapshot
  message. See the README or book_snapshot.clj for more information about why
  this is the case."
  [{:keys [price qty maker-is-bid? tid timestamp snapshot-delay] :as data}]
  (map->Trade data))

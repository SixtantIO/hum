# hum

An experimental library for efficient binary serialization of L2 book data.

## Usage

This library encodes the following data:
  - Order book snapshots (L2)
  - Order book diffs (L2)
  - Trades (TODO)
  - Disconnect events, to differentiate between a few seconds spent reconnecting 
    to the exchange and a few seconds with no activity (TODO)
    

Quick example (see [example with buffers](#example) for something more complete):
```clojure
(require '[io.sixtant.hum :as hum])
(require '[io.sixtant.hum.messages :as messages])

;; An order book snapshot
(def snapshot
  (messages/order-book-snapshot
    {:tick-size 0.50M
     :lot-size  0.1M
     :bids      [{:price 100000.50M :qty 1.2M}]
     :asks      [{:price 102000.50M :qty 50.2M}]
     :timestamp (System/currentTimeMillis)}))

;; An order book diff -- price level at 125,000.00 now has 20.3
(def diff
  (messages/order-book-diff
    {:price 125000.00M
     :qty 20.3M
     :bid? false
     :timestamp (System/currentTimeMillis)}))

;; Write all messages to a single byte array
(hum/write-with hum/writer [snapshot diff])
;=> #object["[B" 0x241fa40 "[B@241fa40"]

(vec *1)
;=> [8 0 0 0 0 1 119 -77 15 -119 114 48 0 0 18 10 5 1 1 1 0 2 12 12 106 24 -10 9 -25 24 68 46 -128 -53 64 66 15]

;; Eagerly read all messages from an input stream or byte array
(hum/read-with hum/reader (byte-array *1))
;=> [#io.sixtant.hum.messages.OrderBookSnapshot{:bids [{:price 100000.5M, :qty 1.2M}],
;                                               :asks [{:price 102000.5M, :qty 50.2M}],
;                                               :timestamp 1613616875890,
;                                               :tick-size 0.5M,
;                                               :lot-size 0.1M}
;    #io.sixtant.hum.messages.OrderBookDiff{:price 125000.0M,
;                                           :qty 20.3M,
;                                           :bid? false,
;                                           :timestamp 1613616887794,
;                                           :snapshot-delay nil}]
```

## Design Goals

Storing data is a tradeoff between processing speed and storage efficiency.
Ideally, this library will be able to:

1. store order books for a cost ~ 10 GB per book per year, and
2. retrieve order books and run calculations (e.g. liquidity within N basis 
   points of the midprice) fast enough to serve as a custom back end for a 
   [Grafana](https://grafana.com/) dashboard — i.e. process queries over an 
   arbitrary time period in several seconds (certainly with reduced granularity 
   as the time period widens; still, seeking through and down-sampling data must 
   sufficiently fast).

If (1) can be achieved, this library can be used with a data collection system
that is tracking hundreds of order books using relatively cheap cloud 
infrastructure for storage. 

If (2) can be achieved, processing speed is fast enough not only to build simple
time series charts with Grafana, but also to replay messages quickly for 
simulation or more complex order book visualization.

This feels like a tall order — I'm still not sure how feasible these goals are,
but I consider them prerequisites for this library's production readiness. 

## How It Works

The writer exposed in this library writes L2 order book data in a custom binary 
format to a filesystem-backed, append-only storage medium. 

Every data file begins with a full order book snapshot, and thereafter changes
to the book are recorded as level diffs, with periodic snapshots. The byte size 
of a level diff is dynamic. It depends on (1) the tick and lot size of the order 
book and (2) the magnitude of the prices and quantities currently in the book. 
To achieve maximal compression, each time a full book snapshot is written, the 
minimal bit lengths necessary to store the maximum price and maximum quantity 
on the book are computed, and subsequent diff messages use those bit lengths. 

This yields much smaller messages, at infinite precision, at the cost of 
constantly risking overflow if a new book level exceeds the maximum price 
or quantity previously seen (in practice this is rare if you have a full view
of the book -- somebody will have always placed a sell order at the very maximum 
price allowed by the exchange). Therefore, for each diff message written, **the
writer must also be prepared to write a full snapshot in the event of an 
overflow for the price or quantity fields.**

So the tradeoff is that the writer must have access to a full order book at all
times, as well as the diff messages (this ought to be the case anyway, but it
does add some complexity).

Additionally, **it's recommended to write snapshots periodically**, independent 
of overflow, to allow faster message replay to reach a certain point in time 
even when using data files spanning days or weeks. E.g. if you store a snapshot 
every hour, you'll never need to replay more than one hour of book diffs to 
construct a book at a specific time, even if your data file contains an entire 
week of book data.

## Messages
The message types and constructors are in [io.sixtant.hum.messages](src/io/sixtant/hum/messages.clj).
They exist for the purpose of efficient binary serialization, not to provide 
the best representation of order book data to work with (e.g. in a diff, the
side is encoded as a clumsy `:bid?` boolean, and in snapshots the book levels
are represented as a vector).

Therefore, **it is highly recommended** that the application producing or 
consuming the book data use its own representation, and only translate to and
from these comparatively low level message types for writing or reading data.

## Example

```clojure
(require '[io.sixtant.hum :as hum])
(require '[io.sixtant.hum.messages :as messages])
(import '(java.io ByteArrayOutputStream ByteArrayInputStreame))

;; An order book snapshot
(def snapshot
  (messages/order-book-snapshot
    {:tick-size 0.50M
     :lot-size  0.1M
     :bids      [{:price 100000.50M :qty 1.2M}]
     :asks      [{:price 102000.50M :qty 50.2M}]
     :timestamp (System/currentTimeMillis)}))

;; An order book diff -- price level at 125,000.00 now has 20.3
(def diff
  (messages/order-book-diff
    {:price 125000.00M
     :qty 20.3M
     :bid? false
     :timestamp (System/currentTimeMillis)}))

;; This could be any OutputStream, but I'll use this to see the bytes as
;; they are written
(def out (ByteArrayOutputStream.))

;; The writer is a java.io.Closeable
(with-open [writer (hum/writer out)]
  ;; To write, just invoke the writer as a function
  (writer snapshot)
  (println "Snapshot bytes" (vec (.toByteArray out)))
  (.reset out)

  (writer diff)
  (println "Diff bytes" (vec (.toByteArray out))))

; Snapshot bytes [8 0 0 0 0 1 119 -77 15 -119 114 48 0 0 18 10 5 1 1 1 0 2 12 12 106 24 -10 9 -25 24]
; Diff bytes [68 46 -128 -53 64 66 15]

;; Reading
(def some-bytes
  [8 0 0 0 0 1 119 -77 15 -119 114 48 0 0 18
   10 5 1 1 1 0 2 12 12 106 24 -10 9 -25 24
   68 46 -128 -53 64 66 15])

(with-open [rdr (hum/reader (ByteArrayInputStream. (byte-array some-bytes)))]
  ;; Invoke the reader to read a single message, or nil if EOF.
  (println "First:" (rdr))
  (println "Second:" (rdr))
  (println "Third:" (rdr)))
; First: #io.sixtant.hum.messages.OrderBookSnapshot{:bids [{:price 100000.5M, :qty 1.2M}], :asks [{:price 102000.5M, :qty 50.2M}], :timestamp 1613616875890, :tick-size 0.5M, :lot-size 0.1M}
; Second: #io.sixtant.hum.messages.OrderBookDiff{:price 125000.0M, :qty 20.3M, :bid? false, :timestamp 1613616887794, :snapshot-delay nil}
; Third: nil
```

Since every diff might contain numbers for price or quantity which are too 
large to write given the field sizes currently in use, **each diff must
include a `:snapshot-delay`** which dereferences to a full, up to date book 
snapshot.

```clojure
;; Instead of constructing your diff message like this...
(def diff
  (messages/order-book-diff
    {:price 125000.00M
     :qty 20.3M
     :bid? false
     :timestamp (System/currentTimeMillis)}))

;; ... construct it like this
(def diff
  (messages/order-book-diff
    {:price 125000.00M
     :qty 20.3M
     :bid? false
     :timestamp (System/currentTimeMillis)
     :snapshot-delay (delay 
                       (potentially-expensive-fn-returing-snapshot))}))
```

That way, when the writer cosumes a book diff, it checks the fields sizes, and 
if they overflow, it simply writes `(-> diff :snapshot-delay deref)` instead.

## Benchmarks

Run at the REPL, using the [io.sixtant.hum.benchmark](src/io/sixtant/hum/benchmark.clj)
namespace, or via `lein bench`.

Here are the benchmark results for the (only) codec, named ARTHUR in honor
of [the most prolific producer](https://www.bitmex.com/) of L2 order book data,
run against the same hour of L2 data for 6 different markets.

```
==========================
BENCHMARK RESULTS (ARTHUR)
==========================

|                   |     ARTHUR (BitMEX XBTUSD) |      ARTHUR (Bitso BTCMXN) |     ARTHUR (Bitso ETHMXN) |     ARTHUR (Bitso BTCARS) |        ARTHUR (FTX BTCUSD) |        ARTHUR (FTX ETHUSD) |
|-------------------+----------------------------+----------------------------+---------------------------+---------------------------+----------------------------+----------------------------|
|    Median Read μs |                       5.49 |                        5.2 |                      5.13 |                      5.62 |                       4.51 |                       4.51 |
|   Median Write μs |                       4.92 |                       5.44 |                      4.88 |                       4.9 |                       4.04 |                        4.0 |
|    Snapshot Bytes |                      75314 |                      43532 |                     25762 |                      7098 |                       1026 |                       1026 |
|        Diff Bytes |                         10 |                          7 |                        11 |                         7 |                          5 |                          8 |
|   Serialized Size |                    4953518 |                    1449792 |                    745949 |                    630158 |                    2041846 |                    1330448 |
|       # of Events |                     521910 |                     143844 |                     77432 |                     67227 |                     294030 |                     186353 |
|    ASCII EDN Size | 50838004 (codec is 0.097x) | 13426947 (codec is 0.108x) | 7139351 (codec is 0.104x) | 6371725 (codec is 0.099x) | 15660696 (codec is 0.130x) | 10525217 (codec is 0.126x) |
|  Gzipped EDN Size |  4936548 (codec is 1.003x) |  1362716 (codec is 1.064x) |  741442 (codec is 1.006x) |  580405 (codec is 1.086x) |  1436078 (codec is 1.422x) |   999134 (codec is 1.332x) |
| GBs / Book / Year |                      43.39 |                       12.7 |                      6.53 |                      5.52 |                      17.89 |                      11.65 |
```

> If gzipped EDN is smaller, why not just use that? This goes back to the 
> tradeoff between size & processing speed -- gzipped EDN does not allow 
> seeking through the file without deserailizing (that's why gzipping is so
> efficient, the encoding relies on knowing the entire contents some message
> before encoding it). We want to store years of order book data and rapidly
> seek to specific locations.

## Next Steps

#### Compression

There are still some compression optimizations that I haven't explored in this
codec. 
- I want to measure how much space is unused in the average level diff or
  removal, because I have a feeling that the average space needed versus 
  maximum theoretical space needed might be a large difference, so the diffs
  could be encoded in one of two sizes (optimistic vs large), and the reader
  would distinguish based on the size of the message.
- Another potential optimization is delta encoding the prices. Most changes are
  near the top of the book, so the writer could encode diff prices as the number
  of ticks from the top of the book (i.e. 0 ticks represents the top level, -1
  ticks represents a new level which undercut the top level by one tick, etc.).
  Those numbers would be smaller than the full number of ticks from zero to the
  price. 

#### Speed

[Real Logic's Simple Binary Encoding (SBE)](https://github.com/real-logic/simple-binary-encoding)
is a library for binary encoding focused on speed. I'm curious if I could use
this at the bottom of the codec, or maybe in a separate speed-focused codec 
(the tradeoff being potentially less compression).

Pending experimentation:
- Test how compact the message format can be made with SBE — can the same 
  bit-level number manipulation be encoded in their XML spec?
- Test the speed of seeking through a SBE-encoded file to a specific point in 
  time. Can I use the same trick that this current codec uses, and decode 
  message frames without actually decoding the messages, so that it's feasible 
  to seek quickly through a file to arrive at a specific timestamp?

## ARTHUR Codec

This is low-level information about the binary serialization format.

Every encoded message sits inside of a message frame, which holds the message
type, size, and timestamp.

### Message Frame

<clojure-docs io.sixtant.hum.arthur.message-frame>
The message frame inside of which all messages nest.

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
        +--------+-----------+---------+---------+--------------+

</clojure-docs>

### Book Snapshot
<clojure-docs io.sixtant.hum.arthur.book-snapshot>
A full order book snapshot message.

Order books have a minimum price increment (tick) and minimum quantity
increment (lot), so this encoding of snapshots uses integer multiples of ticks
and lots instead of decimal prices and quantities (as does the book diff
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
    +------------+-------+----------+

</clojure-docs>

### Level Diff
<clojure-docs io.sixtant.hum.arthur.level-diff>
A change to an order book level.

A level diff is either a [price qty] tuple to mean that the `price` level
now has the new `qty`, or simply a `price` field to indicate a removal.

Both numbers are represented as an integer number of ticks or lots according
to the serialization context map, and have field lengths designated by the
:pbits/:qbits attributes of the serialization context map.

A level diff:

    +-------+-------+
    | Ticks |  Lots |
    | pbits | qbits |
    +-------+-------+

A level removal:

    +-------+
    | Ticks |
    | pbits |
    +-------+

The type of message (ask/bid and diff/removal) is interpreted from the
message type flag in the surrounding message frame.

</clojure-docs>

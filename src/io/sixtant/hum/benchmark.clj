(ns io.sixtant.hum.benchmark
  "Benchmark codecs using test data from one hour of BitMEX XBTUSD L2 data."
  (:require [clojure.java.io :as io]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum :as hum]
            [taoensso.tufte :as tufte]
            [clojure.pprint :as pprint]
            [taoensso.encore :as enc])
  (:import (java.util.zip GZIPInputStream)
           (java.net URL)))


(set! *warn-on-reflection* true)


(defn load-test-data [xf gzipped-resource]
  (with-open [r (-> gzipped-resource
                    (io/input-stream)
                    (GZIPInputStream.)
                    (io/reader))]
    (into [] (comp (map read-string) xf) (line-seq r))))


(defn ->messages
  "Helper to convert messages from EDN to serializable snapshots and diffs."
  [test-datum]
  (case (:type test-datum)
    :diff
    (let [{:keys [asks bids timestamp]} test-datum]
      (concat
        (map #(messages/order-book-diff (assoc % :bid? false :timestamp timestamp)) asks)
        (map #(messages/order-book-diff (assoc % :bid? true :timestamp timestamp)) bids)))

    :snapshot
    (let [{:keys [bids asks tick-size lot-size timestamp]} test-datum]
      [(messages/order-book-snapshot
         {:tick-size tick-size
          :lot-size (messages/adjust-lot-size 1M (concat bids asks))
          :bids bids
          :asks asks
          :timestamp timestamp})])))


;; path->msgs, here as an atom so that it can be easily cleared if necessary
;; (e.g. for custom tests with many resources, which would otherwise be a memory
;; leak)
(defonce cached-msgs (atom {}))


(defn get-msgs [gzipped-resource]
  (let [path (.getFile ^URL gzipped-resource)]
    (if-let [msgs (get @cached-msgs path)]
      msgs
      (do
        (println "Loading L2 data from" (str gzipped-resource) "...")
        (let [result (load-test-data (mapcat ->messages) gzipped-resource)]
          (swap! cached-msgs assoc path result)
          (println "Loaded.")
          result)))))


(defn test-bijective [test-messages serialized-bytes reader]
  (println "Deserializing" (count serialized-bytes) "bytes...")
  (let [dser (hum/read-with reader serialized-bytes)]
    (println "Checking serialization / deserialization bijection...")
    (if (= dser test-messages)
      (println "Success. Deserialized messages match source messages.")
      (let [mismatches (map (fn [src ds]
                              (when-not (= src ds)
                                {:source-msg src
                                 :deserialized ds}))
                            test-messages
                            dser)]
        (when (not= (count dser) (count test-messages))
          (println "WARN: Deserialized message count doesn't match test message count."))
        (throw
          (ex-info
            "Deserialized messages do not match serialized messages!"
            {:examples (into [] (comp (filter some?) (take 5)) mismatches)}))))))


(defn count-ascii-bytes [gzipped-resource]
  (with-open [r (-> gzipped-resource
                    (io/input-stream)
                    (GZIPInputStream.)
                    (io/reader))]
    (transduce
      (map #(count (.getBytes ^String %))) + 0 (line-seq r))))


(defn- run-tests [codec-name reader writer msgs-name msgs-resource]
  (let [test-messages (get-msgs msgs-resource)
        _ (println "Serializing" (count test-messages) "book messages...")
        serialized (hum/write-with writer test-messages)
        _ (test-bijective test-messages serialized reader)

        snapshot-bytes (hum/write-with writer (take 1 test-messages))
        diff-bytes (->> (hum/write-with writer (take 2 test-messages))
                        (drop (count snapshot-bytes))
                        (byte-array))
        header (format "%s (%s)" codec-name msgs-name)
        result (fn [desc val] {"" desc, header val})

        size-on-disk (.length (io/file msgs-resource))
        ascii-size (count-ascii-bytes msgs-resource)]

    [(result "Snapshot Bytes" (count snapshot-bytes))
     (result "Diff Bytes" (count diff-bytes))
     (result "Serialized Size" (count serialized))
     (result "ASCII EDN Size"
             (format "%s (codec is %.3fx)"
                     ascii-size
                     (float (/ (count serialized) ascii-size))))
     (result "Gzipped EDN Size"
             (format "%s (codec is %.3fx)"
                     size-on-disk
                     (float (/ (count serialized) size-on-disk))))
     (result "GBs / Book / Year" (-> (* (count serialized) 24 365)
                                     (/ 1000000000.0)
                                     (enc/round2)))]))


(defn print-title [s]
  (let [divider (apply str (repeat (count s) "="))]
    (println divider)
    (println s)
    (println divider)))


(defn test-codec [codec-name rdr wtr msgs-name msgs-resource]
  (print-title (format "TESTING CODEC '%s' WITH %s" codec-name msgs-name))

  (let [[r pstats] (tufte/profiled {}
                     (run-tests codec-name rdr wtr msgs-name msgs-resource))

        header (format "%s (%s)" codec-name msgs-name)
        result (fn [desc val] {"" desc, header val})]
    (println "\nTufte performance data:")
    (println (tufte/format-pstats pstats))
    (into
      [(result "Median Read μs"
               (-> @pstats :stats :read :p50 (/ 1000.0) enc/round2))
       (result "Median Write μs"
               (-> @pstats :stats :write :p50 (/ 1000.0) enc/round2))]
      r)))


(defn test-codec-with-msgs [names+msgs codec-name reader writer]
  (let [results
        (mapv
          (fn [[msgs-name msgs-resource]]
            (test-codec codec-name reader writer msgs-name msgs-resource))
          names+msgs)]
    (apply map merge results)))


(defn test-codecs [{:keys [codecs msgs]}]
  (let [print-divider (delay (println "* * * * * * * * * * * * * * * * * *"))]
    (->> codecs
         (mapv
           (fn [[codec-name reader writer]]
             [codec-name
              (test-codec-with-msgs msgs codec-name reader writer)]))
         (run!
           (fn [[label results]]
             @print-divider
             (println "")
             (print-title (format "BENCHMARK RESULTS (%s)" label))
             (pprint/print-table results))))))


(comment
  ;; Codec 1 encodes full unsigned long timestamps in book snapshots, and diffs
  ;; get unisgned int timestamp offsets (good for about an hour). Squishes
  ;; price and quantity bits together into the smallest possible byte
  ;; representation.

  ;; Codec 2 improves on Codec 1 by putting timestamp offsets in the message
  ;; frame and making them two bytes wide instead of four, meaning that a
  ;; special timestamp message has to be written approximately once per minute.

  ;; Codec 3 is really gung-ho about bit manipulation, and prepends two fields
  ;; to each diff message specifying the number of bits used by the price / qty
  ;; respectively in that particular message. The idea is that the message size
  ;; changes a lot (often the quantity is just 0), so the messages should prefer
  ;; variable fields to fields which accommodate the max.

  ;; Codec 0 realizes that most of the gains from codec 3 come from the '0'
  ;; quantity fields for level removals, and directly represents a separate,
  ;; much smaller message type for those messages. Because manipulation at the
  ;; bit level is restricted just to unpacking the price and quantity fields,
  ;; it is much faster

  (test-codecs
    {:codecs [["Codec1" hum/reader1 hum/writer1]
              ["Codec2" hum/reader2 hum/writer2]
              ["Codec3" hum/reader3 hum/writer3]
              ["Codec0" hum/reader hum/writer]]
     :msgs   [["BitMEX XBTUSD" (io/resource "bitmex-btc-usd.edn.gz")]
              ["Bitso BTCMXN" (io/resource "bitso-btc-mxn.edn.gz")]
              ["Bitso ETHMXN" (io/resource "bitso-eth-mxn.edn.gz")]
              ["Bitso BTCARS" (io/resource "bitso-btc-ars.edn.gz")]
              ["FTX BTCUSD" (io/resource "ftx-btc-usd.edn.gz")]
              ["FTX ETHUSD" (io/resource "ftx-eth-usd.edn.gz")]]})

  (reset! cached-msgs {})

"
  ==========================
  BENCHMARK RESULTS (Codec1)
  ==========================

  |                   |     Codec1 (BitMEX XBTUSD) |      Codec1 (Bitso BTCMXN) |     Codec1 (Bitso ETHMXN) |     Codec1 (Bitso BTCARS) |        Codec1 (FTX BTCUSD) |        Codec1 (FTX ETHUSD) |
  |-------------------+----------------------------+----------------------------+---------------------------+---------------------------+----------------------------+----------------------------|
  |    Median Read μs |                       6.21 |                       6.14 |                      5.99 |                      6.05 |                       5.72 |                       5.71 |
  |   Median Write μs |                        6.4 |                       5.86 |                      5.73 |                      5.92 |                       5.08 |                       5.39 |
  |    Snapshot Bytes |                      75309 |                      43527 |                     25757 |                      7093 |                       1021 |                       1021 |
  |        Diff Bytes |                         13 |                         14 |                        13 |                        13 |                         10 |                         10 |
  |   Serialized Size |                    6860126 |                    2057329 |                   1032360 |                    881031 |                    2941311 |                    1864541 |
  |    ASCII EDN Size | 50838004 (codec is 0.135x) | 13426947 (codec is 0.153x) | 7139351 (codec is 0.145x) | 6371725 (codec is 0.138x) | 15660696 (codec is 0.188x) | 10525217 (codec is 0.177x) |
  |  Gzipped EDN Size |  4936548 (codec is 1.390x) |  1362716 (codec is 1.510x) |  741442 (codec is 1.392x) |  580405 (codec is 1.518x) |  1436078 (codec is 2.048x) |   999134 (codec is 1.866x) |
  | GBs / Book / Year |                      60.09 |                      18.02 |                      9.04 |                      7.72 |                      25.77 |                      16.33 |

  ==========================
  BENCHMARK RESULTS (Codec2)
  ==========================

  |                   |     Codec2 (BitMEX XBTUSD) |      Codec2 (Bitso BTCMXN) |     Codec2 (Bitso ETHMXN) |     Codec2 (Bitso BTCARS) |        Codec2 (FTX BTCUSD) |        Codec2 (FTX ETHUSD) |
  |-------------------+----------------------------+----------------------------+---------------------------+---------------------------+----------------------------+----------------------------|
  |    Median Read μs |                       7.29 |                       7.49 |                      7.32 |                      7.28 |                        7.0 |                        7.0 |
  |   Median Write μs |                       6.41 |                       6.59 |                       6.5 |                      6.33 |                       5.52 |                       5.55 |
  |    Snapshot Bytes |                      75314 |                      43532 |                     25762 |                      7098 |                       1026 |                       1026 |
  |        Diff Bytes |                         11 |                         12 |                        11 |                        11 |                          8 |                          8 |
  |   Serialized Size |                    5816907 |                    1770242 |                    878097 |                    747178 |                    2353852 |                    1492436 |
  |    ASCII EDN Size | 50838004 (codec is 0.114x) | 13426947 (codec is 0.132x) | 7139351 (codec is 0.123x) | 6371725 (codec is 0.117x) | 15660696 (codec is 0.150x) | 10525217 (codec is 0.142x) |
  |  Gzipped EDN Size |  4936548 (codec is 1.178x) |  1362716 (codec is 1.299x) |  741442 (codec is 1.184x) |  580405 (codec is 1.287x) |  1436078 (codec is 1.639x) |   999134 (codec is 1.494x) |
  | GBs / Book / Year |                      50.96 |                      15.51 |                      7.69 |                      6.55 |                      20.62 |                      13.07 |

  ==========================
  BENCHMARK RESULTS (Codec3)
  ==========================

  |                   |     Codec3 (BitMEX XBTUSD) |      Codec3 (Bitso BTCMXN) |     Codec3 (Bitso ETHMXN) |     Codec3 (Bitso BTCARS) |        Codec3 (FTX BTCUSD) |        Codec3 (FTX ETHUSD) |
  |-------------------+----------------------------+----------------------------+---------------------------+---------------------------+----------------------------+----------------------------|
  |    Median Read μs |                       7.92 |                       8.07 |                      8.01 |                      8.17 |                       7.55 |                       7.52 |
  |   Median Write μs |                       6.44 |                       6.57 |                      6.68 |                      7.21 |                       6.34 |                       6.17 |
  |    Snapshot Bytes |                      75314 |                      43532 |                     25762 |                      7098 |                       1026 |                       1026 |
  |        Diff Bytes |                         10 |                          8 |                        11 |                         9 |                          7 |                          8 |
  |   Serialized Size |                    4894311 |                    1476944 |                    794148 |                    684865 |                    2239353 |                    1415368 |
  |    ASCII EDN Size | 50838004 (codec is 0.096x) | 13426947 (codec is 0.110x) | 7139351 (codec is 0.111x) | 6371725 (codec is 0.107x) | 15660696 (codec is 0.143x) | 10525217 (codec is 0.134x) |
  |  Gzipped EDN Size |  4936548 (codec is 0.991x) |  1362716 (codec is 1.084x) |  741442 (codec is 1.071x) |  580405 (codec is 1.180x) |  1436078 (codec is 1.559x) |   999134 (codec is 1.417x) |
  | GBs / Book / Year |                      42.87 |                      12.94 |                      6.96 |                       6.0 |                      19.62 |                       12.4 |

  ==========================
  BENCHMARK RESULTS (Codec0)
  ==========================

  |                   |     Codec0 (BitMEX XBTUSD) |      Codec0 (Bitso BTCMXN) |     Codec0 (Bitso ETHMXN) |     Codec0 (Bitso BTCARS) |        Codec0 (FTX BTCUSD) |        Codec0 (FTX ETHUSD) |
  |-------------------+----------------------------+----------------------------+---------------------------+---------------------------+----------------------------+----------------------------|
  |    Median Read μs |                       4.85 |                        5.0 |                      4.96 |                      4.91 |                       4.66 |                        4.7 |
  |   Median Write μs |                       4.24 |                       4.83 |                      4.74 |                      4.75 |                       4.04 |                       4.04 |
  |    Snapshot Bytes |                      75314 |                      43532 |                     25762 |                      7098 |                       1026 |                       1026 |
  |        Diff Bytes |                         10 |                          7 |                        11 |                         7 |                          5 |                          8 |
  |   Serialized Size |                    4953518 |                    1449792 |                    745949 |                    630158 |                    2041846 |                    1330448 |
  |    ASCII EDN Size | 50838004 (codec is 0.097x) | 13426947 (codec is 0.108x) | 7139351 (codec is 0.104x) | 6371725 (codec is 0.099x) | 15660696 (codec is 0.130x) | 10525217 (codec is 0.126x) |
  |  Gzipped EDN Size |  4936548 (codec is 1.003x) |  1362716 (codec is 1.064x) |  741442 (codec is 1.006x) |  580405 (codec is 1.086x) |  1436078 (codec is 1.422x) |   999134 (codec is 1.332x) |
  | GBs / Book / Year |                      43.39 |                       12.7 |                      6.53 |                      5.52 |                      17.89 |                      11.65 |
"
  )


(comment
  "To trim test resources to one hour intervals"

  (defn first-n-milliseconds [rdr ms-interval]
    (let [first-line (read-string (.readLine rdr))
          cutoff (+ (:timestamp first-line) ms-interval)]
      (cons
        first-line
        (sequence
          (comp
            (map read-string)
            (take-while #(<= (:timestamp %) cutoff)))
          (line-seq rdr)))))

  ;; Trim a bunch of files to only use one hour of test data
  (doseq [f ["bitmex-btc-usd"
             "bitso-btc-ars" "bitso-btc-mxn" "bitso-eth-mxn"
             "ftx-btc-usd" "ftx-eth-usd"]]
    (with-open [r (io/reader (str "resources/" f ".edn"))
                w (io/writer (str "resources/trimmed/" f ".edn"))]
      (binding [*out* w]
        (run! prn (first-n-milliseconds r (enc/ms :hours 1))))))

  )

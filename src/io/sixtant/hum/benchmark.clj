(ns io.sixtant.hum.benchmark
  "Benchmark codecs using one hour of L2 data from various markets."
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum :as hum]
            [taoensso.tufte :as tufte]
            [taoensso.encore :as enc])
  (:import (java.util.zip GZIPInputStream)
           (java.net URL)
           (java.io ByteArrayOutputStream)))


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
     (result "# of Events" (count test-messages))
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


(defn -main []
  (test-codecs
    {:codecs [["ARTHUR" hum/reader hum/writer]]
     :msgs   [["BitMEX XBTUSD" (io/resource "bitmex-btc-usd.edn.gz")]
              ["Bitso BTCMXN" (io/resource "bitso-btc-mxn.edn.gz")]
              ["Bitso ETHMXN" (io/resource "bitso-eth-mxn.edn.gz")]
              ["Bitso BTCARS" (io/resource "bitso-btc-ars.edn.gz")]
              ["FTX BTCUSD" (io/resource "ftx-btc-usd.edn.gz")]
              ["FTX ETHUSD" (io/resource "ftx-eth-usd.edn.gz")]]}))


(comment
  (-main)

  "
  ==========================
  BENCHMARK RESULTS (ARTHUR)
  ==========================

  |                   |     ARTHUR (BitMEX XBTUSD) |      ARTHUR (Bitso BTCMXN) |     ARTHUR (Bitso ETHMXN) |     ARTHUR (Bitso BTCARS) |        ARTHUR (FTX BTCUSD) |        ARTHUR (FTX ETHUSD) |
  |-------------------+----------------------------+----------------------------+---------------------------+---------------------------+----------------------------+----------------------------|
  |    Median Read μs |                        4.0 |                       4.25 |                      4.14 |                      4.17 |                       3.89 |                       3.91 |
  |   Median Write μs |                       3.94 |                       3.92 |                      5.24 |                      3.89 |                       3.83 |                       3.69 |
  |    Snapshot Bytes |                      75315 |                      43533 |                     25763 |                      7099 |                       1027 |                       1027 |
  |        Diff Bytes |                          9 |                          7 |                        10 |                         7 |                          5 |                          7 |
  |   Serialized Size |                    4372399 |                    1333106 |                    704919 |                    579876 |                    1798450 |                    1155458 |
  |       # of Events |                     521910 |                     143844 |                     77432 |                     67227 |                     294030 |                     186353 |
  |    ASCII EDN Size | 50838004 (codec is 0.086x) | 13426947 (codec is 0.099x) | 7139351 (codec is 0.099x) | 6371725 (codec is 0.091x) | 15660696 (codec is 0.115x) | 10525217 (codec is 0.110x) |
  |  Gzipped EDN Size |  4936548 (codec is 0.886x) |  1362716 (codec is 0.978x) |  741442 (codec is 0.951x) |  580405 (codec is 0.999x) |  1436078 (codec is 1.252x) |   999134 (codec is 1.156x) |
  | GBs / Book / Year |                       38.3 |                      11.68 |                      6.18 |                      5.08 |                      15.75 |                      10.12 |
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

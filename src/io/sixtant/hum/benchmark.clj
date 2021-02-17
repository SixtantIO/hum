(ns io.sixtant.hum.benchmark
  "Benchmark codecs using test data from one hour of BitMEX XBTUSD L2 data."
  (:require [clojure.java.io :as io]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum :as hum]
            [taoensso.tufte :as tufte]
            [clojure.pprint :as pprint]
            [taoensso.encore :as enc]
            [org.clojars.smee.binary.core :as b]
            [io.sixtant.hum.uint :as uint])
  (:import (java.util.zip GZIPInputStream)
           (java.io EOFException ByteArrayInputStream)))


(set! *warn-on-reflection* true)


(def test-data (io/resource "bitmex-btc-usd.edn.gz"))


(defn load-test-data [xf]
  (with-open [r (-> test-data
                    (io/input-stream)
                    (GZIPInputStream.)
                    (io/reader))]
    (into [] (comp (map read-string) xf) (line-seq r))))


(defn ->messages
  "Helper to convert messages from EDN to serializable snapshots and diffs."
  [test-datum]
  (case (:type test-datum)
    :diff
    (let [{:keys [asks bids ts]} test-datum]
      (concat
        (map #(messages/order-book-diff (assoc % :bid? false :timestamp ts)) asks)
        (map #(messages/order-book-diff (assoc % :bid? true :timestamp ts)) bids)))

    :snapshot
    (let [{:keys [bids asks tick-size lot-size ts]} test-datum]
      [(messages/order-book-snapshot
         {:tick-size tick-size
          :lot-size (messages/adjust-lot-size 1M (concat bids asks))
          :bids bids
          :asks asks
          :timestamp ts})])))


(defonce test-messages
  (do
    (println "Loading L2 data from" (str test-data) "...")
    (let [result (load-test-data (mapcat ->messages))]
      (println "Loaded.")
      result)))



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


(defn- run-tests [reader writer test-messages codec-name]
  (println "Serializing" (count test-messages) "book messages...")
  (let [serialized (hum/write-with writer test-messages)
        _ (test-bijective test-messages serialized reader)

        snapshot-bytes (hum/write-with writer (take 1 test-messages))
        diff-bytes (->> (hum/write-with writer (take 2 test-messages))
                        (drop (count snapshot-bytes))
                        (byte-array))
        result (fn [desc val] {"" desc, codec-name val})

        size-on-disk (.length (io/file test-data))
        ascii-size (with-open [r (-> test-data
                                     (io/input-stream)
                                     (GZIPInputStream.)
                                     (io/reader))]
                     (transduce
                       (map #(count (.getBytes ^String %))) + 0 (line-seq r)))]

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


(defn test-codec [reader writer codec-name]
  (let [s (str "TESTING CODEC " codec-name)
        s' (apply str (repeat (count s) "="))]
    (println s')
    (println s)
    (println s'))

  (let [[r pstats] (tufte/profiled {} (run-tests reader writer test-messages codec-name))
        result (fn [desc val] {"" desc, codec-name val})]
    (println "\nTufte performance data:")
    (println (tufte/format-pstats pstats))
    (into
      [(result "Mean Read μs"
               (-> @pstats :stats :read :mean (/ 1000.0) enc/round2))
       (result "Mean Write μs"
               (-> @pstats :stats :write :mean (/ 1000.0) enc/round2))]
      r)))


(defn test-codecs [& reader+writer+names]
  (let [tables
        (vec (for [[reader writer codec-name] reader+writer+names]
               (test-codec reader writer codec-name)))]
    (println "=================")
    (println "BENCHMARK RESULTS")
    (println "=================")
    (pprint/print-table (apply map merge tables))))


(comment

  (test-codecs [hum/reader4 hum/writer4 "Codec4"])

  (test-codecs
    [hum/reader1 hum/writer1 "Codec1"]
    [hum/reader2 hum/writer2 "Codec2"]
    [hum/reader3 hum/writer3 "Codec3"]
    [hum/reader4 hum/writer4 "Codec4"])

  "
  =================
  BENCHMARK RESULTS
  =================

  |                   |                     Codec1 |                     Codec2 |                     Codec3 |
  |-------------------+----------------------------+----------------------------+----------------------------|
  |      Mean Read μs |                       6.44 |                       7.77 |                       9.18 |
  |     Mean Write μs |                       6.69 |                       6.46 |                       7.42 |
  |    Snapshot Bytes |                      64988 |                      64993 |                      64993 |
  |        Diff Bytes |                         12 |                         10 |                         10 |
  |   Serialized Size |                    4966388 |                    4460716 |                    4169562 |
  |    ASCII EDN Size | 32066369 (codec is 0.155x) | 32066369 (codec is 0.139x) | 32066369 (codec is 0.130x) |
  |  Gzipped EDN Size |  3813523 (codec is 1.302x) |  3813523 (codec is 1.170x) |  3813523 (codec is 1.093x) |
  | GBs / Book / Year |                      43.51 |                      39.08 |                      36.53
  "
  )

(ns io.sixtant.hum.benchmark
  "Benchmark codecs using test data from one hour of BitMEX XBTUSD L2 data."
  (:require [clojure.java.io :as io]
            [io.sixtant.hum.messages :as messages]
            [io.sixtant.hum :as hum]
            [taoensso.tufte :as tufte]
            [clojure.pprint :as pprint]
            [taoensso.encore :as enc])
  (:import (java.util.zip GZIPInputStream)))


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
        (throw
          (ex-info
            "Deserialized messages do not match serialized messages!"
            {:examples (into [] (comp (filter some?) (take 5)) mismatches)}))))))


(defn- run-tests [reader writer test-messages]
  (println "Serializing" (count test-messages) "book messages...")
  (let [serialized (hum/write-with writer test-messages)
        _ (test-bijective test-messages serialized reader)

        snapshot-bytes (hum/write-with writer (take 1 test-messages))
        diff-bytes (->> (hum/write-with writer (take 2 test-messages))
                        (drop (count snapshot-bytes))
                        (byte-array))
        result (fn [desc val] {"" desc, " " val})

        size-on-disk (.length (io/file test-data))
        ratio (* (/ (count serialized) size-on-disk) 100.0)]

    [(result "Snapshot Bytes" (count snapshot-bytes))
     (result "Diff Bytes" (count diff-bytes))
     (result "Total Size" (count serialized))
     (result "Gzipped EDN Size" size-on-disk)
     (result "Serialized Size" (format "%.0f%%" ratio))]))


(defn test-codec
  [reader writer]
  (let [[r pstats] (tufte/profiled {} (run-tests reader writer test-messages))
        result (fn [desc val] {"" desc, " " val})]
    (println "\nTufte performance data:")
    (println (tufte/format-pstats pstats))
    (print "\nCodec results:")
    (pprint/print-table
      (into
        [(result "Mean Read μs"
                 (-> @pstats :stats :read :mean (/ 1000.0) enc/round2))
         (result "Mean Write μs"
                 (-> @pstats :stats :write :mean (/ 1000.0) enc/round2))]
        r))))


(comment
  (test-codec hum/l2-reader hum/l2-writer)
  (require 'criterium.core)
  (criterium.core/quick-bench
    (read-string "{:type :diff, :market \"XBTUSD\", :asks [{:price 39389.5M, :qty 0.075045M}], :bids [], :ts 1612655525335}\n"))
  )

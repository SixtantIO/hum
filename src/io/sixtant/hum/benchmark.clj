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


(defn- run-tests [codec-name reader writer msgs-resource]
  (let [test-messages (get-msgs msgs-resource)
        _ (println "Serializing" (count test-messages) "book messages...")
        serialized (hum/write-with writer test-messages)
        _ (test-bijective test-messages serialized reader)

        snapshot-bytes (hum/write-with writer (take 1 test-messages))
        diff-bytes (->> (hum/write-with writer (take 2 test-messages))
                        (drop (count snapshot-bytes))
                        (byte-array))
        result (fn [desc val] {"" desc, codec-name val})

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


(defn test-codec [codec-name reader writer msgs-name msgs-resource]
  (print-title (format "TESTING CODEC '%s' WITH %s" codec-name msgs-name))

  (let [[r pstats] (tufte/profiled {}
                     (run-tests codec-name reader writer msgs-resource))
        result (fn [desc val] {"" desc, codec-name val})]
    (println "\nTufte performance data:")
    (println (tufte/format-pstats pstats))
    (into
      [(result "Mean Read μs"
               (-> @pstats :stats :read :mean (/ 1000.0) enc/round2))
       (result "Mean Write μs"
               (-> @pstats :stats :write :mean (/ 1000.0) enc/round2))]
      r)))


(defn test-codecs-with-messages
  [codecs msgs-name msgs-resource]
  (->> codecs
       (mapv
         (fn [[codec-name reader writer]]
           (test-codec codec-name reader writer msgs-name msgs-resource)))
       (apply map merge)))


(defn test-codecs [{:keys [codecs msgs]}]
  (->> msgs
       (mapv
         (fn [[label rsource]]
           [label (test-codecs-with-messages codecs label rsource)]))
       (run!
         (fn [[label results]]
           (print-title (format "BENCHMARK RESULTS (%s)" label))
           (pprint/print-table results)))))


(comment

  (test-codecs
    {:codecs [["Codec1" hum/reader1 hum/writer1]
              ["Codec2" hum/reader2 hum/writer2]
              ["Codec3" hum/reader3 hum/writer3]
              ["Codec4" hum/reader4 hum/writer4]]
     :msgs   [["BitMEX XBTUSD" (io/resource "bitmex-btc-usd.edn.gz")]]})

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

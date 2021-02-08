(ns io.sixtant.hum-test
  (:require [clojure.test :refer :all]
            [io.sixtant.hum :refer :all]
            [io.sixtant.hum.messages :as messages]
            [clojure.java.io :as io]))


(def micro-timestamp (* (System/currentTimeMillis) 1000))


(def snap
  (messages/order-book-snapshot
    {:tick-size 0.01M
     :lot-size 0.000001M
     :bids [{:price 100000.52M :qty 1.2345M}]
     :asks [{:price 102000.52M :qty 50.2345M}]
     :timestamp micro-timestamp}))


(def diff-a
  (messages/order-book-diff
    {:price 125000.01M
     :qty 20.3045M
     :bid? false
     :timestamp (+ micro-timestamp 100)}))


(def diff-b
  (messages/order-book-diff
    {:price 102000.52M
     :qty 2.35M
     :bid? true
     :timestamp (+ micro-timestamp 200)}))


(deftest serialization-test
  (is (= (->> [snap diff-a diff-b]
              (write-with writer1)
              (read-with reader1))
         [snap diff-a diff-b])
      "Messages survive the serialize -> deserialize loop intact.")

  (let [empty-snap (write-with writer1 [(assoc snap :bids [] :asks [])])]
    (println (count empty-snap) "bytes / empty snapshot"))

  (let [snap-bytes (write-with writer1 [snap])
        snap+diff (write-with writer1 [snap diff-a])]
    (println (- (count snap+diff) (count snap-bytes)) "bytes / diff")))

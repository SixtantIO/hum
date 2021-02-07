(ns io.sixtant.hum-test
  (:require [clojure.test :refer :all]
            [io.sixtant.hum :refer :all]
            [io.sixtant.hum.messages :as messages]))


(def micro-timestamp (* (System/currentTimeMillis) 1000))


(def snap
  (messages/order-book-snapshot
    {:tick-size 0.01M
     :lot-size 0.0001M
     :bids [{:price 100.52M :qty 1.2345M}]
     :asks [{:price 102.52M :qty 5.2345M}]
     :timestamp micro-timestamp}))


(def diff-a
  (messages/order-book-diff
    {:price 125.01M
     :qty 2.3045M
     :bid? false
     :timestamp (+ micro-timestamp 100)}))


(def diff-b
  (messages/order-book-diff
    {:price 102.52M
     :qty 2.35M
     :bid? true
     :timestamp (+ micro-timestamp 200)}))


(deftest serialization-test
  (is (= (->> [snap diff-a diff-b]
              (write-with l2-writer)
              (read-with l2-reader))
         [snap diff-a diff-b])
      "Messages survive the serialize -> deserialize loop intact."))

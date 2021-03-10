(ns io.sixtant.hum-test
  (:require [clojure.test :refer :all]
            [io.sixtant.hum :refer :all]
            [io.sixtant.hum.messages :as messages]))


(def timestamp (System/currentTimeMillis))


(def snap
  (messages/order-book-snapshot
    {:tick-size 0.01M
     :lot-size  0.000001M
     :bids      [{:price 100000.52M :qty 1.2345M}]
     :asks      [{:price 102000.52M :qty 50.2345M}]
     :timestamp timestamp}))


(def diff-a
  (messages/order-book-diff
    {:price 125000.01M
     :qty 20.3045M
     :bid? false
     :timestamp (+ timestamp 100)}))


(def diff-b
  (messages/order-book-diff
    {:price 102000.52M
     :qty 2.35M
     :bid? true
     :timestamp (+ timestamp 200)}))


(def rem-a (-> (assoc diff-a :qty 0M) (update :timestamp + 200)))
(def rem-b (-> (assoc diff-b :qty 0M) (update :timestamp + 200)))


(deftest serialization-test
  (is (= (->> [snap diff-a diff-b rem-a rem-b]
              (write-with writer)
              (read-with reader))
         [snap diff-a diff-b rem-a rem-b])
      "Messages survive the serialize -> deserialize loop intact.")

  (let [empty-snap (write-with writer [(assoc snap :bids [] :asks [])])]
    (println (count empty-snap) "bytes / empty snapshot"))

  (let [snap-bytes (write-with writer [snap])
        snap+diff (write-with writer [snap diff-a])]
    (println (- (count snap+diff) (count snap-bytes)) "bytes / diff"))

  (let [snap-bytes (write-with writer [snap])
        snap+rem (write-with writer [snap rem-a])]
    (println (- (count snap+rem) (count snap-bytes)) "bytes / removal")))


(def big-number 100000000000000000000000000000000000000000000000000000000000M)


(deftest overflow-test
  (testing "when :price overflows"

    (testing "and there is no snapshot-delay"
      (let [diff (assoc diff-a :price big-number)]
        (is
          (thrown?
            IllegalArgumentException
            #"Diffs must include a :snapshot-delay so that.*"
            (write-with writer [snap diff])))))

    (testing "and there is a snapshot-delay"
      (let [diff (assoc diff-a :price big-number :snapshot-delay (delay snap))]
        (is (= (->> [snap diff]
                    (write-with writer)
                    (read-with reader))
               [snap (assoc snap :timestamp (:timestamp diff))])
            "a new snapshot is written instead of the invalid diff")))))

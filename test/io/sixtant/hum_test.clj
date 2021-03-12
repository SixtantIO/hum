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


(def bid-hit-trade
  (messages/trade
    {:price 100000.52M
     :qty 0.52M
     :maker-is-bid? true
     :tid 26558224
     :timestamp (+ timestamp 300)}))


(def quote-lifted-trade
  (messages/trade
    {:price 102000.52M
     :qty 0.02345M
     :maker-is-bid? false
     :tid "9c5d7509-3c2b-4769-81fe-9915f5dd9515"
     :timestamp (+ timestamp 400)}))


(def disconnect (messages/disconnect {:timestamp (+ timestamp 500)}))
(def rem-a (-> (assoc diff-a :qty 0M) (update :timestamp + 200)))
(def rem-b (-> (assoc diff-b :qty 0M) (update :timestamp + 200)))



(deftest serialization-test
  (let [msgs [snap ; snapshot
              diff-a diff-b  ; book diffs
              bid-hit-trade quote-lifted-trade ; trades
              rem-a rem-b  ; book level removals
              disconnect]] ; disconnect message
    (is (= (->> msgs (write-with writer) (read-with reader)) msgs)
        "Messages survive the serialize -> deserialize loop intact."))

  (let [empty-snap (write-with writer [(assoc snap :bids [] :asks [])])]
    (println (count empty-snap) "bytes / empty snapshot"))

  (let [snap-bytes (write-with writer [snap])
        snap+diff (write-with writer [snap diff-a])]
    (println (- (count snap+diff) (count snap-bytes)) "bytes / diff"))

  (let [snap-bytes (write-with writer [snap])
        snap+rem (write-with writer [snap rem-a])]
    (println (- (count snap+rem) (count snap-bytes)) "bytes / removal"))

  (let [snap-bytes (write-with writer [snap])
        snap+trade (write-with writer [snap bid-hit-trade])]
    (println (- (count snap+trade) (count snap-bytes)) "bytes / trade")))


(def big-number 100000000000000000000000000000000000000000000000000000000000M)


(deftest overflow-test
  (testing "when writing a level diff"
    (testing "and :price overflows"

      (testing "and there is no snapshot-delay"
        (let [diff (assoc diff-a :price big-number)]
          (is
            (thrown?
              IllegalArgumentException
              #"Messages must include a :snapshot-delay so that.*"
              (write-with writer [snap diff])))))

      (testing "and there is a snapshot-delay"
        (let [diff (assoc diff-a :price big-number :snapshot-delay (delay snap))]
          (is (= (->> [snap diff]
                      (write-with writer)
                      (read-with reader))
                 [snap (assoc snap :timestamp (:timestamp diff))])
              "a new snapshot is written instead of the invalid diff")))))

  (testing "when writing a trade"
    (doseq [attr [:price :qty]]
      (testing (str "and " attr " overflows")

        (testing "and there is no snapshot-delay"
          (let [trade (assoc bid-hit-trade attr big-number)]
            (is
              (thrown?
                IllegalArgumentException
                #"Messages must include a :snapshot-delay so that.*"
                (write-with writer [snap trade])))))

        (testing "and there is a snapshot-delay"
          (let [trade (assoc bid-hit-trade
                        attr big-number
                        :snapshot-delay (delay snap))]
            (is (= (->> [snap trade]
                        (write-with writer)
                        (read-with reader))
                   [snap
                    (assoc snap :timestamp (:timestamp trade))
                    (assoc trade :snapshot-delay nil)])
                "a new snapshot is written before the trade")))))))

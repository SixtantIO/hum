(ns io.sixtant.hum.uint
  "For binary encoding vectors of unsigned, arbitrary bit precision integers.

  Useful for packing a series of integers whose bit lengths are not multiples
  of 8 into the most compact byte representation possible.")


(set! *warn-on-reflection* true)


(defn pack-two-ints
  "Pack two unsigned integers into one integer whose right `right-bit-length`
  bits represent the `right` integer, with all remaining bits to the left
  representing the `left` integer."
  [^BigInteger left ^BigInteger right right-bit-length]
  (let [mask (fn [bl] (.subtract (.pow BigInteger/TWO (max bl 0)) BigInteger/ONE))]
    (when-not (= (.and right (mask right-bit-length)) right)
      (throw (ArithmeticException.
               (format "Integer %s does not fit inside of %s bits."
                       right right-bit-length)))))
  (.or right (.shiftLeft left right-bit-length)))


(defn unpack-two-ints
  "Unpack two unsigned integers from the unsigned integer `n`, returning
  [left right].

  The `right` integer is represented by the right `right-bit-length` bits of
  `n`, with all remaining bits to the left representing the `left` integer."
  [^BigInteger n right-bit-length]
  (let [mask (fn [bl] (.subtract (.pow BigInteger/TWO (max bl 0)) BigInteger/ONE))]
    [(.and (.shiftRight n right-bit-length)
           (mask (- (.bitLength n) right-bit-length)))
     (.and n (mask right-bit-length))]))


(comment
  ;; Pack two integers together into one. The right-most `right-bits` of the
  ;; resulting binary represent the right number, and those left over represent
  ;; the left number.
  (let [right-bits 3
        packed (pack-two-ints (biginteger 2) (biginteger 5) right-bits)
        binary-string (fn [i] (.toString i 2))]
    (binary-string packed))
  ;=> "10101"
  ;      101 <- the right number, 5
  ;    10    <- the left number, 2


  ;; Notice that the bitlength of the left integer doesn't matter while packing
  ;; -- only the right needs to be specified.
  (pack-two-ints (biginteger 424242) (biginteger 5) 3)
  ;=> 3393941

  ;; Again, specify only the number of right-hand bits to unpack, then the left
  ;; number is whatever is left over.
  (unpack-two-ints (biginteger 3393941) 3)
  ;=> [424242 5]
  )


(defn pack-ints
  "Pack an arbitrary number of unsigned integers into a single integer, such
  that the result is a concatenation of the bits of each integer in order from
  left to right.

  Integers are padded according to the specified `bit-lengths`. The first
  bit-length (for the left-most integer) is implicit given the other bit
  lengths, so you may supply a nil value.

  E.g. (pack-ints (mapv biginteger [23429234 25 42]) [:n 5 6]) ;=> 47983072874"
  [ints bit-lengths]
  (assert (= (count ints) (count bit-lengths)))
  (loop [^BigInteger i (first ints)
         ints (rest ints)
         bit-lengths (rest bit-lengths)] ; bit-length of left most int ignored
    (if-let [next-int (first ints)]
      (recur
        (pack-two-ints i next-int (first bit-lengths))
        (rest ints)
        (rest bit-lengths))
      i)))


(defn unpack-ints
  "Unpack unsigned integers from the single unsigned integer `i`, according to
  the specified `bit-lengths` in order from left to right.

  The first  bit-length (for the left-most integer) is implicit given the other
  bit lengths, so you may supply a nil value.

  E.g. (unpack-ints (biginteger 47983072874) [:n 5 6]) ;=> [23429234 25 42]"
  [i bit-lengths]
  (loop [ints (list) ; append to front, i.e. in reverse order
         bit-lengths (reverse (rest bit-lengths))
         ^BigInteger i i]
    (if-let [bl (first bit-lengths)]
      (let [[i right] (unpack-two-ints i bl)]
        (recur (conj ints right) (rest bit-lengths) i))
      (vec (conj ints i)))))


(comment
  (let [packed (pack-ints (mapv biginteger [3 5 25]) [:n 3 5])
        binary-string (fn [i] (.toString i 2))]
    (binary-string packed))
  ;=> "1110111001"
  ;         11001 <- third number, 25
  ;      101      <- second number, 5
  ;    11         <- first number, 3

  (pack-ints (mapv biginteger [42422 5 25]) [:n 3 5])
  ;=> 10860217

  (unpack-ints (biginteger 10860217) [:n 3 5])
  ;=> [42422 5 25]
  )

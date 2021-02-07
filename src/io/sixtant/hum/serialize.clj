(ns io.sixtant.hum.serialize
  (:import (java.io OutputStream InputStream ByteArrayOutputStream)))


(defprotocol DTO
  (serialize [this ^OutputStream out]
    "Serialize the fields represented in the DTO to binary and write the bytes
    to the given OutputStream.")
  (deserialize [this ^InputStream in]
    "Read from the given InputStream and populate the record fields."))


(defmacro with-bytes [stream-binding & body]
  `(let [~stream-binding (ByteArrayOutputStream.)]
     (do ~@body)
     (vec (.toByteArray ~stream-binding))))

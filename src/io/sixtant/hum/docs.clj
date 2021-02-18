(ns io.sixtant.hum.docs
  "Helper to extract namespace documentation and inject it in README files.

  Allows a text file to include <clojure-docs> tags which indicate sections of
  the text file to keep in sync with namespace doc strings. E.g.

      ## My Readme

      The best namespace in the project is `my.namespace`.

      <clojure-docs my.namespace>
      This text will be replaced with the documentation for `my.namespace`.
      </clojure-docs>

  Then, if you run `(replace-clojure-docs! \"README.md\")`, the file is updated
  to include the namespace docstrings."
  (:require [clojure.string :as string]))


(defn replace-between [token-fn replace-fn xs]
  (loop [out []
         xs xs]
    (if-let [x (first xs)]
      (if (token-fn x)
        (let [contents (into [x] (take-while (complement token-fn)) (rest xs))
              end-token (nth xs (count contents))]
          (recur
            (into out (replace-fn x (conj contents end-token)))
            (drop (+ (count contents) 1) xs)))
        (recur (conj out x) (rest xs)))
      out)))


(defn doc-tag?
  "Truthy for some tag like `<clojure-docs my.namespace>` or `</clojure-docs>`.

  Returns the namespace string for the former, or `true` for the latter."
  [s]
  (let [open-tag "<clojure-docs "
        close-tag "</clojure-docs>"]
    (cond
      (string/starts-with? s open-tag) (subs s (count open-tag) (dec (count s)))
      (string/starts-with? s close-tag) true
      :else nil)))


(defn ns-docs-for [ns-string]
  (let [ns* (-> ns-string symbol)]
    (require ns* :reload)
    (-> ns*
        find-ns
        meta
        :doc)))


(comment
  ;; E.g.
  (replace-between
    #{"?"}
    (fn [_ inside]
      (println "Replacing:" inside)
      ["*" "REPLACED" "*"])
    ["Some text preceding start of block"
     "?"
     "inside" "the" "block"
     "?"
     "more text outside the block"
     "then a second block"
     "?"
     "second block contents"
     "?"])
  ;=> ["Some text preceding start of block"
  ;    "?"
  ;    "REPLACED"
  ;    "block"
  ;    "more text outside the block"
  ;    "then a second block"
  ;    "?"
  ;    "REPLACED"
  ;    "second block contents"
  )


(defn include-namespace-docs
  "For use with `replace-between`."
  [clojure-docs-token _]
  (let [ns (doc-tag? clojure-docs-token)
        ns-doc-lines (string/split-lines (ns-docs-for ns))
        trim-two-spaces (map #(if (> (count %) 2) (subs % 2) %))
        ns-doc-lines (into [(first ns-doc-lines)]
                           trim-two-spaces
                           (rest ns-doc-lines))]
    ;; Keep the open and close tokens surrounding the docs
    (-> (into [clojure-docs-token] ns-doc-lines)
        (conj "</clojure-docs>"))))


(defn add-ns-docs [readme-lines]
  (str
    (->> readme-lines
         (replace-between (comp some? doc-tag?) include-namespace-docs)
         (string/join "\n")
         "\n")))


(defn replace-clojure-docs! [path]
  (->> (slurp path)
       (string/split-lines)
       (add-ns-docs)
       (spit path)))


;; Overwrite the current README with a new one, whose <clojure-doc /> tags are
;; populated.
(defn -main [] (replace-clojure-docs! "README.md"))

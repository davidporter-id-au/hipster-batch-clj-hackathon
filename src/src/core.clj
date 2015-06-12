(ns src.core (:gen-class))
(require '[aws.sdk.s3 :as s3])
(require '[clojure.data.json :as json])

(def cred { :access-key "something-with-s3-permissions" :secret-key "aws-secret-key" } )
(def bucket "hipsterbatchhack")

;Mutable current state
(def *currentPointer* nil)

(defn listContents [s3Key]
    (map (fn [ea] (:key ea)) (:objects (s3/list-objects cred bucket [:prefix s3Key]))))

(defn getFile [s3Key]
    (slurp (:content (s3/get-object cred bucket s3Key))))

(defn readPage [s3Key]
    (json/read-str (getFile s3Key) :key-fn keyword))

(def config (json/read-str (getFile "v1.config") :key-fn keyword))

(defn processFile [event]
  (println "Processing file" (:Key event))
  ;FIXME: do something with the file retrieved
  (getFile (:Key event))
)

(defn processPageContents [page]
  (if (contains? page :Items)
    (pmap processFile (:Items page))))


(defn getListOfPagesFromPosition [s3Key]
    (println "Accessing " s3Key)
    (try
        (let [currentPage (readPage s3Key)]

            (println "Processing page" s3Key)
            (processPageContents currentPage)

            (if (contains? currentPage :Next)

                ;If there is a next, return a list of this key plus the next recursively
                (concat (conj '() s3Key) (getListOfPagesFromPosition (:Next currentPage)))

                ;else just return the current s3Key
                '()
            )
        )

    ;FIXME handle exceptions a little better than simply blanket ignoring them
    (catch Exception e (println "Key not available. At end of event list." (.getMessage e)))))

(defn fetchAndUpdate [s3Key]
    (def *currentPointer* (last (getListOfPagesFromPosition s3Key))))

(defn -main
  [& args]

  (fetchAndUpdate (:start (:event config)))

  (while true
    ;sleep for a little before polling again
    (print "current pointer is" *currentPointer*)
    (Thread/sleep (* 1000 5))

    (fetchAndUpdate *currentPointer*)
  )
)

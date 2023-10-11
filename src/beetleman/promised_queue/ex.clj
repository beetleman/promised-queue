(ns beetleman.promised-queue.ex
  (:require [promesa.core :as p]))

(defn queue-closed-ex []
  (ex-info "Queue closed!" {:type ::closed}))

(defn queue-closed! [d]
  (p/reject! d (queue-closed-ex)))

(defn queue-closed-ex? [ex]
  (= ::closed
     (-> ex ex-data :type)))

(comment
  (queue-closed-ex? (queue-closed-ex)))

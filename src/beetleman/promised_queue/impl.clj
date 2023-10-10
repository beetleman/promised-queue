(ns beetleman.promised_queue.impl
  (:require [promesa.core :as p]))

(defn- process-state [{:keys [items-q deferred-q]}]
  (loop [items-q    items-q
         deferred-q deferred-q]
    (if (or (empty? items-q)
            (empty? deferred-q))
      {:items-q    items-q
       :deferred-q deferred-q}
      (do
        (p/resolve! (peek deferred-q) (peek items-q))
        (recur (pop items-q)
               (pop deferred-q))))))

(defn create [items]
  (agent {:items-q (into clojure.lang.PersistentQueue/EMPTY items)
          :deferred-q clojure.lang.PersistentQueue/EMPTY
          :closed?    false}))

(defn closed? [queue]
  (:closed? @queue))

(defn queue-closed-ex []
  (ex-info "Queue closed!" {:type ::closed}))

(defn queue-closed! [d]
  (p/reject! d (queue-closed-ex)))

(defn queue-closed-ex? [ex]
  (= ::closed
     (-> ex ex-data :type)))

(comment
  (queue-closed-ex? (queue-closed-ex)))

(defn close! [queue]
  (let [d (p/deferred)]
    (send queue
          (fn [q]
            (if (:closed? q)
              q
              (-> q
                  (update :deferred-q
                          (fn [deferred]
                            (doseq [d deferred]
                              (queue-closed! d))
                            clojure.lang.PersistentQueue/EMPTY))
                  (assoc :closed? true)))))
    d))

(defn enqueue! [queue item]
  (let [d (p/deferred)]
    (send queue
          (fn [q]
            (if (:closed? q)
              (queue-closed! d)
              (let [q (-> q
                          (update :items-q conj item)
                          process-state)]
                (p/resolve! d item)
                q))))
    d))

(defn dequeue! [queue]
  (let [d (p/deferred)]
    (send queue
          (fn [q]
            (if (:closed? q)
              (queue-closed! d)
              (-> q
                  (update :deferred-q conj d)
                  process-state))))
    d))

(comment
  (let [queue (create [])]
    (future
      (try
        (loop [item @(dequeue! queue)]
          (println "Item:" item)
          (recur @(dequeue! queue)))
        (catch Exception e
          (if (queue-closed-ex? (ex-cause e))
            (println "stop listening")
            (println e)))))
    (future
      (dotimes [i 10]
        (enqueue! queue i)
        (Thread/sleep 100))
      (println "stop sending")
      (Thread/sleep 1000)
      (close! queue))))

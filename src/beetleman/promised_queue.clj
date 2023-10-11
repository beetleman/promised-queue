(ns beetleman.promised-queue
  (:require [beetleman.promised-queue.ex :as ex]
            [beetleman.promised-queue.memory :as memory]
            [beetleman.promised-queue.proto :as proto]))

(defn create [items & {:keys [type] :or {type :memory}}]
  (case type
    :memory (memory/create items)))

(defn enqueue! [queue item]
  (proto/-enqueue! queue item))

(defn dequeue! [queue]
  (proto/-dequeue! queue))

(defn close! [queue]
  (.close queue))

(defn get-info [queue]
  (proto/-get-info queue))

(defn closed? [queue]
  (-> queue get-info :closed?))

(defn length [queue]
  (-> queue get-info :length))

(comment
  (with-open [queue (create [])]
    (future
      (try
        (loop [item @(dequeue! queue)]
          (println "Item:" item)
          (recur @(dequeue! queue)))
        (catch Exception e
          (if (ex/queue-closed-ex? (ex-cause e))
            (println "stop listening")
            (println e)))))
    (future
      (dotimes [i 10]
        (enqueue! queue i)
        (println (get-info queue))
        (Thread/sleep 100))
      (println "stop sending"))
    (Thread/sleep 1000)))

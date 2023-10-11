(ns beetleman.promised-queue.memory
  (:require [promesa.core :as p]
            [beetleman.promised-queue.proto :as proto]
            [beetleman.promised-queue.ex :as ex])
  (:import [java.io Closeable]))

(defn- process-state [{:keys [items-q deferred-q] :as state}]
  (loop [items-q    items-q
         deferred-q deferred-q]
    (if (or (empty? items-q)
            (empty? deferred-q))
      (assoc state
             :items-q    items-q
             :deferred-q deferred-q)
      (do
        (p/resolve! (peek deferred-q) (peek items-q))
        (recur (pop items-q)
               (pop deferred-q))))))

(defn close! [state-agent]
  (let [d (p/deferred)]
    (send state-agent
          (fn [state]
            (if (:closed? state)
              state
              (-> state
                  (update :deferred-q
                          (fn [deferred]
                            (doseq [d deferred]
                              (ex/queue-closed! d))
                            clojure.lang.PersistentQueue/EMPTY))
                  (assoc :closed? true)))))
    d))

(defn enqueue! [queue item]
  (let [d (p/deferred)]
    (send queue
          (fn [q]
            (if (:closed? q)
              (ex/queue-closed! d)
              (let [q (-> q
                          (update :items-q conj item)
                          process-state)]
                (p/resolve! d item)
                q))))
    d))

(defn dequeue! [state-agent]
  (let [d (p/deferred)]
    (send state-agent
          (fn [state]
            (if (:closed? state)
              (ex/queue-closed! d)
              (-> state
                  (update :deferred-q conj d)
                  process-state))))
    d))

(defn get-info [state-agent]
  (let [{:keys [closed? items-q deferred-q]} @state-agent]

    {:closed? closed?
     :length (count items-q)
     :awaited (count deferred-q)}))

(defn create [items]
  (let [state-agent (agent {:items-q    (into clojure.lang.PersistentQueue/EMPTY items)
                            :deferred-q clojure.lang.PersistentQueue/EMPTY
                            :closed?    false})]
    (reify
      proto/PromisedQueue
      (-enqueue! [_ item]
        (enqueue! state-agent item))
      (-dequeue! [_]
        (dequeue! state-agent))
      (-get-info [_]
        (get-info state-agent))
      Closeable
      (close [_]
        (close! state-agent)))))

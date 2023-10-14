(ns beetleman.promised-queue-test
  (:require [clojure.test :refer :all]
            [beetleman.promised-queue :as promised-queue]))

(deftest memory-queue-test
  (testing "Queue test"
    (with-open [queue (promised-queue/create [] {})]
      (let [size           10000
            items          (range size)
            worker-state-1 (atom [])
            worker-state-2 (atom [])
            workers-state  [worker-state-1 worker-state-2]
            workers        (for [worker-state workers-state]
                             (future
                               (loop [r @(promised-queue/dequeue! queue)]
                                 (when-not (= r ::stop)
                                   (swap! worker-state conj r)
                                   (recur @(promised-queue/dequeue! queue))))))]
        (doseq [d (doall (for [items (partition-all (int (/ size 10)) items)]
                           (future
                             (doseq [item items]
                               (promised-queue/enqueue! queue item)))))]
          @d)
        (doseq [_ workers]
          (promised-queue/enqueue! queue ::stop))
        (doseq [w workers]
          (is (not= (deref w 10000 ::fail)
                    ::fail)))
        (let [items-from-queue (into []
                                     (mapcat deref)
                                     workers-state)]
          (is (= size
                 (count items-from-queue)))
          (is (= (sort items-from-queue)
                 (sort items))))))))

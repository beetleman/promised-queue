(ns beetleman.promised-queue.proto)

(defprotocol PromisedQueue
  (-enqueue! [this item])
  (-dequeue! [this])
  (-get-info [this]))

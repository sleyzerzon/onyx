(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.messaging.acking-daemon :as acker]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.operation :as operation]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre :refer [debug info]]
            [onyx.types :refer [->Leaf]])
  (:import [java.util UUID]))

(defn into-transient [coll vs]
  (loop [rs (seq vs) updated-coll coll]
    (if rs 
      (recur (next rs) 
             (conj! updated-coll (first rs)))
      updated-coll)))

(defn fast-concat [vvs]
  (loop [vs (seq vvs) coll (transient [])]
    (if vs
      (recur (next vs) 
             (into-transient coll (first vs)))
      (persistent! coll))))

;; needs a performance boost
(defn build-segments-to-send [leaves]
  (->> leaves
       (map (fn [{:keys [routes ack-vals hash-group message] :as leaf}]
              (if (= :retry (:action routes))
                []
                (map (fn [route ack-val]
                       (->Leaf (:message leaf)
                               (:id leaf)
                               (:acker-id leaf)
                               (:completion-id leaf)
                               ack-val
                               nil
                               route
                               nil
                               (get hash-group route)))
                     (:flow routes) 
                     ack-vals))))
       fast-concat))

(defn pick-peer [id active-peers hash-group max-downstream-links]
  (when-not (empty? active-peers)
    (if hash-group
      (nth active-peers
           (mod (hash hash-group)
                (count active-peers)))
      (rand-nth (operation/select-n-peers id active-peers max-downstream-links)))))

(defn read-batch [{:keys [onyx.core/messenger] :as event}]
  {:onyx.core/batch (onyx.extensions/receive-messages messenger event)})

(defn write-batch 
  [{:keys [onyx.core/id onyx.core/results 
           onyx.core/messenger onyx.core/job-id 
           onyx.core/max-downstream-links] :as event}]
    (let [leaves (fast-concat (map :leaves results))
          egress-tasks (:egress-ids (:onyx.core/serialized-task event))]
      (when-not (empty? leaves)
        (let [replica-val @(:onyx.core/replica event)
              peer-state (:peer-state replica-val)
              segments (build-segments-to-send leaves)
              groups (group-by #(list (:route %) (:hash-group %)) segments)
              allocations (get (:allocations replica-val) job-id)]
          (doseq [[[route hash-group] segs] groups]
            (let [peers (get allocations (get egress-tasks route))
                  active-peers (filter #(= (peer-state %) :active) peers)
                  target (pick-peer id active-peers hash-group max-downstream-links)]
              (when target
                (let [link (operation/peer-link replica-val (:onyx.core/state event) event target)]
                  (onyx.extensions/send-messages messenger event link segs)))))
          {}))))

(defrecord Function [replica state id messenger job-id max-downstream-links egress-tasks]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    {:onyx.core/batch (onyx.extensions/receive-messages messenger event)})

  (write-batch 
    [_ {:keys [onyx.core/results] :as event}]
    (let [leaves (fast-concat (map :leaves results))]
      (when-not (empty? leaves)
        (let [replica-val @replica
              peer-state (:peer-state replica-val)
              segments (build-segments-to-send leaves)
              groups (group-by #(list (:route %) (:hash-group %)) segments)
              allocations (get (:allocations replica-val) job-id)]
          (doseq [[[route hash-group] segs] groups]
            (let [peers (get allocations (get egress-tasks route))
                  active-peers (filter #(= (peer-state %) :active) peers)
                  target (pick-peer id active-peers hash-group max-downstream-links)]
              (when target
                (let [link (operation/peer-link replica-val state event target)]
                  (onyx.extensions/send-messages messenger event link segs)))))
          {}))))

  (seal-resource [_ _]
    nil))

(defn function [{:keys [onyx.core/replica
                        onyx.core/state
                        onyx.core/id 
                        onyx.core/messenger 
                        onyx.core/job-id 
                        onyx.core/max-downstream-links
                        onyx.core/serialized-task] :as pipeline-data}]
  (->Function replica
              state
              id
              messenger
              job-id
              max-downstream-links
              (:egress-ids serialized-task)))

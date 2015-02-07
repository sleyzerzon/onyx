(ns onyx.plugin.core-async
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout go <!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :refer [debug] :as timbre]))

(defmethod l-ext/inject-lifecycle-resources :core.async/read-from-chan
  [_ event]
  {:core.async/pending-messages (atom {})
   :core.async/replay-ch (chan 1000)})

(defmethod p-ext/read-segment [:input :core.async]
  [{:keys [core.async/in-chan core.async/replay-ch
           core.async/pending-messages] :as event} timeout-ch]
  (let [msg (first (alts!! [replay-ch in-chan timeout-ch] :priority true))]
    (when msg
      (let [rets {:id (java.util.UUID/randomUUID) :input :core.async :message msg}]
        (swap! pending-messages assoc (:id rets) (:message rets))
        rets))))

(defmethod p-ext/decompress-segment [:input :core.async]
  [event segment]
  segment)

(defmethod p-ext/apply-fn [:input :core.async]
  [event segment]
  segment)

(defmethod p-ext/apply-fn [:output :core.async]
  [event segment]
  segment)

(defmethod p-ext/compress-segment [:output :core.async]
  [event segment]
  segment)

(defmethod p-ext/write-segment [:output :core.async]
  [{:keys [core.async/out-chan]} segment]
  (>!! out-chan (:message segment)))

(defmethod p-ext/seal-resource [:output :core.async]
  [{:keys [core.async/out-chan]}]
  (>!! out-chan :done)
  {})

(defmethod p-ext/ack-message [:input :core.async]
  [{:keys [core.async/pending-messages]} message-id]
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/replay-message [:input :core.async]
  [{:keys [core.async/pending-messages core.async/replay-ch]} message-id]
  (>!! replay-ch (get @pending-messages message-id))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/pending? [:input :core.async]
  [{:keys [core.async/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? [:input :core.async]
  [{:keys [core.async/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (vals x)) :done))))

(defn take-segments!
  "Takes segments off the channel until :done is found.
   Returns a seq of segments, including :done."
  [ch]
  (loop [x []]
    (let [segment (<!! ch)]
      (let [stack (conj x segment)]
        (if-not (= segment :done)
          (recur stack)
          stack)))))


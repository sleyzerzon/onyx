(ns onyx.peer.pipeline-extensions
  "Public API extensions for the virtual peer data pipeline.")

(defn task-type [task-map]
  (if (or (:onyx/group-by-key task-map) (:onyx/group-by-fn task-map))
    :aggregator
    (:onyx/type task-map)))

(defn type-and-medium-dispatch [{:keys [onyx.core/task-map]}]
  [(task-type task-map) (:onyx/medium task-map)])

(defmulti read-segment
  (fn [event timeout-ch]
    (type-and-medium-dispatch event)))

(defmulti decompress-segment
  (fn [event segment]
    (type-and-medium-dispatch event)))

(defmulti apply-fn
  "Applies a function to a decompressed segment. Returns a segment
   or vector of new segments."
  (fn [event segment]
    (type-and-medium-dispatch event)))

(defmulti compress-segment
  (fn [event segment]
    (type-and-medium-dispatch segment)))

(defmulti write-segment
  "Writes segments to the outgoing data source. Must return a map."
  (fn [event segment]
    (type-and-medium-dispatch segment)))

(defmulti seal-resource
  "Closes any resources that remain open during a task being executed.
   Called once at the end of a task for each virtual peer after the incoming
   queue has been exhausted. Only called once globally for a single task."
  type-and-medium-dispatch)

(defmulti ack-message
  "Acknowledges a message at the native level for a batch of message ids.
   Must return a map."
  (fn [event message-id]
    (type-and-medium-dispatch event)))

(defmulti replay-message
  "Releases a message id from storage and replays it."
  (fn [event message-id]
    (type-and-medium-dispatch event)))

(defmulti pending?
  "Returns true if this message ID is pending."
  (fn [event messsage-id]
    (type-and-medium-dispatch event)))

(defmulti drained?
  "Returns true if this input resource has been exhausted."
  type-and-medium-dispatch)


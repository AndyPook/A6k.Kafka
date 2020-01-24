namespace A6k.Kafka
{
    /// <summary>
    /// from rd_kafka_resp_err_t
    /// (librdkafka prefixes these with "RD_KAFKA_RESP_ERR_"
    /// </summary>
    public enum ResponseError : short
    {
        /* Internal errors to rdkafka: */
        /** Begin internal error codes */
        BEGIN = -200,
        /** Received message is incorrect */
        BAD_MSG = -199,
        /** Bad/unknown compression */
        BAD_COMPRESSION = -198,
        /** Broker is going away */
        DESTROY = -197,
        /** Generic failure */
        FAIL = -196,
        /** Broker transport failure */
        TRANSPORT = -195,
        /** Critical system resource */
        CRIT_SYS_RESOURCE = -194,
        /** Failed to resolve broker */
        RESOLVE = -193,
        /** Produced message timed out*/
        MSG_TIMED_OUT = -192,
        /** Reached the end of the topic+partition queue on
         * the broker. Not really an error. 
         * This event is disabled by default,
         * see the `enable.partition.eof` configuration property. */
        PARTITION_EOF = -191,
        /** Permanent: Partition does not exist in cluster. */
        UNKNOWN_PARTITION = -190,
        /** File or filesystem error */
        FS = -189,
        /** Permanent: Topic does not exist in cluster. */
        UNKNOWN_TOPIC = -188,
        /** All broker connections are down. */
        ALL_BROKERS_DOWN = -187,
        /** Invalid argument, or invalid configuration */
        INVALID_ARG = -186,
        /** Operation timed out */
        TIMED_OUT = -185,
        /** Queue is full */
        QUEUE_FULL = -184,
        /** ISR count < required.acks */
        ISR_INSUFF = -183,
        /** Broker node update */
        NODE_UPDATE = -182,
        /** SSL error */
        SSL = -181,
        /** Waiting for coordinator to become available. */
        WAIT_COORD = -180,
        /** Unknown client group */
        UNKNOWN_GROUP = -179,
        /** Operation in progress */
        IN_PROGRESS = -178,
        /** Previous operation in progress, wait for it to finish. */
        PREV_IN_PROGRESS = -177,
        /** This operation would interfere with an existing subscription */
        EXISTING_SUBSCRIPTION = -176,
        /** Assigned partitions (rebalance_cb) */
        ASSIGN_PARTITIONS = -175,
        /** Revoked partitions (rebalance_cb) */
        REVOKE_PARTITIONS = -174,
        /** Conflicting use */
        CONFLICT = -173,
        /** Wrong state */
        STATE = -172,
        /** Unknown protocol */
        UNKNOWN_PROTOCOL = -171,
        /** Not implemented */
        NOT_IMPLEMENTED = -170,
        /** Authentication failure*/
        AUTHENTICATION = -169,
        /** No stored offset */
        NO_OFFSET = -168,
        /** Outdated */
        OUTDATED = -167,
        /** Timed out in queue */
        TIMED_OUT_QUEUE = -166,
        /** Feature not supported by broker */
        UNSUPPORTED_FEATURE = -165,
        /** Awaiting cache update */
        WAIT_CACHE = -164,
        /** Operation interrupted (e.g., due to yield)) */
        INTR = -163,
        /** Key serialization error */
        KEY_SERIALIZATION = -162,
        /** Value serialization error */
        VALUE_SERIALIZATION = -161,
        /** Key deserialization error */
        KEY_DESERIALIZATION = -160,
        /** Value deserialization error */
        VALUE_DESERIALIZATION = -159,
        /** Partial response */
        PARTIAL = -158,
        /** Modification attempted on read-only object */
        READ_ONLY = -157,
        /** No such entry / item not found */
        NOENT = -156,
        /** Read underflow */
        UNDERFLOW = -155,
        /** Invalid type */
        INVALID_TYPE = -154,
        /** Retry operation */
        RETRY = -153,
        /** Purged in queue */
        PURGE_QUEUE = -152,
        /** Purged in flight */
        PURGE_INFLIGHT = -151,
        /** Fatal error: see rd_kafka_fatal_error() */
        FATAL = -150,
        /** Inconsistent state */
        INCONSISTENT = -149,
        /** Gap-less ordering would not be guaranteed if proceeding */
        GAPLESS_GUARANTEE = -148,
        /** Maximum poll interval exceeded */
        MAX_POLL_EXCEEDED = -147,
        /** Unknown broker */
        UNKNOWN_BROKER = -146,

        /** End internal error codes */
        END = -100,

        /* Kafka broker errors: */
        /** Unknown broker error */
        UNKNOWN = -1,
        /** Success */
        NO_ERROR = 0,
        /** Offset out of range */
        OFFSET_OUT_OF_RANGE = 1,
        /** Invalid message */
        INVALID_MSG = 2,
        /** Unknown topic or partition */
        UNKNOWN_TOPIC_OR_PART = 3,
        /** Invalid message size */
        INVALID_MSG_SIZE = 4,
        /** Leader not available */
        LEADER_NOT_AVAILABLE = 5,
        /** Not leader for partition */
        NOT_LEADER_FOR_PARTITION = 6,
        /** Request timed out */
        REQUEST_TIMED_OUT = 7,
        /** Broker not available */
        BROKER_NOT_AVAILABLE = 8,
        /** Replica not available */
        REPLICA_NOT_AVAILABLE = 9,
        /** Message size too large */
        MSG_SIZE_TOO_LARGE = 10,
        /** StaleControllerEpochCode */
        STALE_CTRL_EPOCH = 11,
        /** Offset metadata string too large */
        OFFSET_METADATA_TOO_LARGE = 12,
        /** Broker disconnected before response received */
        NETWORK_EXCEPTION = 13,

        /** Coordinator load in progress */
        COORDINATOR_LOAD_IN_PROGRESS = 14,
        /** Group coordinator load in progress */
        //#define GROUP_LOAD_IN_PROGRESS COORDINATOR_LOAD_IN_PROGRESS
        GROUP_LOAD_IN_PROGRESS = 14,

        /** Coordinator not available */
        COORDINATOR_NOT_AVAILABLE = 15,
        /** Group coordinator not available */
        //#define GROUP_COORDINATOR_NOT_AVAILABLE COORDINATOR_NOT_AVAILABLE
        GROUP_COORDINATOR_NOT_AVAILABLE = 15,

        /** Not coordinator */
        NOT_COORDINATOR = 16,
        /** Not coordinator for group */
        //#define NOT_COORDINATOR_FOR_GROUP NOT_COORDINATOR
        NOT_COORDINATOR_FOR_GROUP = 16,

        /** Invalid topic */
        TOPIC_EXCEPTION = 17,
        /** Message batch larger than configured server segment size */
        RECORD_LIST_TOO_LARGE = 18,
        /** Not enough in-sync replicas */
        NOT_ENOUGH_REPLICAS = 19,
        /** Message(s) written to insufficient number of in-sync replicas */
        NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
        /** Invalid required acks value */
        INVALID_REQUIRED_ACKS = 21,
        /** Specified group generation id is not valid */
        ILLEGAL_GENERATION = 22,
        /** Inconsistent group protocol */
        INCONSISTENT_GROUP_PROTOCOL = 23,
        /** Invalid group.id */
        INVALID_GROUP_ID = 24,
        /** Unknown member */
        UNKNOWN_MEMBER_ID = 25,
        /** Invalid session timeout */
        INVALID_SESSION_TIMEOUT = 26,
        /** Group rebalance in progress */
        REBALANCE_IN_PROGRESS = 27,
        /** Commit offset data size is not valid */
        INVALID_COMMIT_OFFSET_SIZE = 28,
        /** Topic authorization failed */
        TOPIC_AUTHORIZATION_FAILED = 29,
        /** Group authorization failed */
        GROUP_AUTHORIZATION_FAILED = 30,
        /** Cluster authorization failed */
        CLUSTER_AUTHORIZATION_FAILED = 31,
        /** Invalid timestamp */
        INVALID_TIMESTAMP = 32,
        /** Unsupported SASL mechanism */
        UNSUPPORTED_SASL_MECHANISM = 33,
        /** Illegal SASL state */
        ILLEGAL_SASL_STATE = 34,
        /** Unuspported version */
        UNSUPPORTED_VERSION = 35,
        /** Topic already exists */
        TOPIC_ALREADY_EXISTS = 36,
        /** Invalid number of partitions */
        INVALID_PARTITIONS = 37,
        /** Invalid replication factor */
        INVALID_REPLICATION_FACTOR = 38,
        /** Invalid replica assignment */
        INVALID_REPLICA_ASSIGNMENT = 39,
        /** Invalid config */
        INVALID_CONFIG = 40,
        /** Not controller for cluster */
        NOT_CONTROLLER = 41,
        /** Invalid request */
        INVALID_REQUEST = 42,
        /** Message format on broker does not support request */
        UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
        /** Policy violation */
        POLICY_VIOLATION = 44,
        /** Broker received an out of order sequence number */
        OUT_OF_ORDER_SEQUENCE_NUMBER = 45,
        /** Broker received a duplicate sequence number */
        DUPLICATE_SEQUENCE_NUMBER = 46,
        /** Producer attempted an operation with an old epoch */
        INVALID_PRODUCER_EPOCH = 47,
        /** Producer attempted a transactional operation in an invalid state */
        INVALID_TXN_STATE = 48,
        /** Producer attempted to use a producer id which is not
         *  currently assigned to its transactional id */
        INVALID_PRODUCER_ID_MAPPING = 49,
        /** Transaction timeout is larger than the maximum
         *  value allowed by the broker's max.transaction.timeout.ms */
        INVALID_TRANSACTION_TIMEOUT = 50,
        /** Producer attempted to update a transaction while another
         *  concurrent operation on the same transaction was ongoing */
        CONCURRENT_TRANSACTIONS = 51,
        /** Indicates that the transaction coordinator sending a
         *  WriteTxnMarker is no longer the current coordinator for a
         *  given producer */
        TRANSACTION_COORDINATOR_FENCED = 52,
        /** Transactional Id authorization failed */
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,
        /** Security features are disabled */
        SECURITY_DISABLED = 54,
        /** Operation not attempted */
        OPERATION_NOT_ATTEMPTED = 55,
        /** Disk error when trying to access log file on the disk */
        KAFKA_STORAGE_ERROR = 56,
        /** The user-specified log directory is not found in the broker config */
        LOG_DIR_NOT_FOUND = 57,
        /** SASL Authentication failed */
        SASL_AUTHENTICATION_FAILED = 58,
        /** Unknown Producer Id */
        UNKNOWN_PRODUCER_ID = 59,
        /** Partition reassignment is in progress */
        REASSIGNMENT_IN_PROGRESS = 60,
        /** Delegation Token feature is not enabled */
        DELEGATION_TOKEN_AUTH_DISABLED = 61,
        /** Delegation Token is not found on server */
        DELEGATION_TOKEN_NOT_FOUND = 62,
        /** Specified Principal is not valid Owner/Renewer */
        DELEGATION_TOKEN_OWNER_MISMATCH = 63,
        /** Delegation Token requests are not allowed on this connection */
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64,
        /** Delegation Token authorization failed */
        DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65,
        /** Delegation Token is expired */
        DELEGATION_TOKEN_EXPIRED = 66,
        /** Supplied principalType is not supported */
        INVALID_PRINCIPAL_TYPE = 67,
        /** The group is not empty */
        NON_EMPTY_GROUP = 68,
        /** The group id does not exist */
        GROUP_ID_NOT_FOUND = 69,
        /** The fetch session ID was not found */
        FETCH_SESSION_ID_NOT_FOUND = 70,
        /** The fetch session epoch is invalid */
        INVALID_FETCH_SESSION_EPOCH = 71,
        /** No matching listener */
        LISTENER_NOT_FOUND = 72,
        /** Topic deletion is disabled */
        TOPIC_DELETION_DISABLED = 73,
        /** Leader epoch is older than broker epoch */
        FENCED_LEADER_EPOCH = 74,
        /** Leader epoch is newer than broker epoch */
        UNKNOWN_LEADER_EPOCH = 75,
        /** Unsupported compression type */
        UNSUPPORTED_COMPRESSION_TYPE = 76,
        /** Broker epoch has changed */
        STALE_BROKER_EPOCH = 77,
        /** Leader high watermark is not caught up */
        OFFSET_NOT_AVAILABLE = 78,
        /** Group member needs a valid member ID */
        MEMBER_ID_REQUIRED = 79,
        /** Preferred leader was not available */
        PREFERRED_LEADER_NOT_AVAILABLE = 80,
        /** Consumer group has reached maximum size */
        GROUP_MAX_SIZE_REACHED = 81,
        /** Static consumer fenced by other consumer with same
         *  group.instance.id. */
        FENCED_INSTANCE_ID = 82,

        END_ALL,
    }
}

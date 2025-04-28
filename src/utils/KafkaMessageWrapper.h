class KafkaMessageWrapper {
public:
    explicit KafkaMessageWrapper(rd_kafka_message_t* msg) : msg_(msg) {}
    ~KafkaMessageWrapper() {
        if (msg_) rd_kafka_message_destroy(msg_);
    }

    KafkaMessageWrapper(KafkaMessageWrapper&& other) noexcept : msg_(other.msg_) {
        other.msg_ = nullptr;
    }

    KafkaMessageWrapper& operator=(KafkaMessageWrapper&& other) noexcept {
        if (this != &other) {
            if (msg_) rd_kafka_message_destroy(msg_);
            msg_ = other.msg_;
            other.msg_ = nullptr;
        }
        return *this;
    }

    rd_kafka_message_t* get() const { return msg_; }

    // prevent copy
    KafkaMessageWrapper(const KafkaMessageWrapper&) = delete;
    KafkaMessageWrapper& operator=(const KafkaMessageWrapper&) = delete;

private:
    rd_kafka_message_t* msg_;
};


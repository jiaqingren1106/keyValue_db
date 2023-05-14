#pragma once

class StorageType {
    public:
        virtual void Init(char* db_name, db_config_t config) = 0;
        virtual void Flush(kv_pair *data, int data_size) = 0;
        virtual val_t Get(key_t key) = 0;
        virtual std::vector<kv_pair> Scan(key_t key1, key_t key2) = 0;
};
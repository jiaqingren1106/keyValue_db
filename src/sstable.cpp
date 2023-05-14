#include "../include/sstable.h"
#include <cstddef>
#include <cstring>
#include <unistd.h>

namespace {
    // return page number if query_key is found, otherwise return -1
    // if is_scan is true, even if the query_key does not exist, we will still return the page number
    // of the page that might contain query_key (i.e. query_key resides in the key range of the page).
    int binary_search_page_of_key(kv_pair* page, key_t query_key, int dataSize, int file, bool is_scan) {
        
        int num_page = (dataSize -1) / PAGE_SIZE + 1;
        int start = 0;
        int end = num_page - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            
            memset(page, TOMBSTONE, PAGE_SIZE);
            int read_size = PAGE_SIZE;
            if ((mid+1)*PAGE_SIZE > dataSize) {
                // last page might not be of size PAGE_SIZE
                read_size = PAGE_SIZE - (num_page*PAGE_SIZE - dataSize);
            }
            int bytes_read = pread(file, page, read_size, sizeof(sst_metadata_t) + mid*PAGE_SIZE);
            
            int num_entry = bytes_read / sizeof(kv_pair);
            key_t first_key = page[0].key;
            key_t last_key = page[num_entry-1].key;
            if (first_key <= query_key && last_key >= query_key) {
                if (!is_scan) { 
                    for (int i = 0; i < num_entry; i++) {
                        if (page[i].key == query_key) {
                            return mid;
                        }
                    }
                    return -1;
                }
                return mid;
            } else if (last_key < query_key) {
                start = mid + 1;
            } else { // first_key > query_key
                end = mid - 1;
            }
        }
        return -1;
    }

    int b_tree_search_page_of_key(uint8_t* b_tree_nodes, int b_tree_size, key_t query_key, bool is_scan) {
        int key_idx = BPTree::searchBTree(b_tree_nodes, b_tree_size, query_key, is_scan);
        if (key_idx != -1) {
            // found
            int page_idx = (key_idx * sizeof(kv_pair)) / PAGE_SIZE;
            return page_idx;
        }
        return -1;
    }
}

sstable::sstable(std::string filename, int ver, db_config_t config) { 
    this->config = config;
    this->filename = filename; 
    this->version = ver;

    // open file
    std::ifstream file(this->filename, std::ios::binary);
    // get its size:
    file.seekg(0, std::ios::end);
    this->data_size = file.tellg();
    // reset seek cursor back to beginning
    file.seekg(0, std::ios::beg);
    file.read((char*)&(this->file_info), sizeof(sst_metadata_t));
    file.close();
//   printf("SST file %s is of size %d\n", this->filename.c_str(), fileSize);
}

sstable sstable::create_sst(kv_pair *data, int data_size, const char* directory, int cur_version, db_config_t config) {
    
    std::string dir = directory;
    std::string filename = dir + "/" + std::to_string(cur_version);

    std::fstream wf(filename, std::ios::out | std::ios::binary);
    if (!wf) {
        std::cout << "Cannot open file!" << std::endl;
    }
    // create metadata for this SST file
    sst_metadata_t metadata;
    metadata.num_entries = data_size / sizeof(kv_pair);
    if (config.bits_per_entry == 0) {
        metadata.bloom_filter_size = 0;
    } else {
        metadata.bloom_filter_size = (metadata.num_entries * config.bits_per_entry - 1) / 8 + 1;// in terms of bytes
    }
    wf.write((char*) &metadata, sizeof(sst_metadata_t));
    wf.write((char *)data, data_size);
    
    // std::cout << "bloom filter size: " << metadata.bloom_filter_size << std::endl;
    if (config.bits_per_entry > 0) {
        // create bloom filter from memory
        BloomFilter filter(metadata.num_entries, config.bits_per_entry);
        uint8_t filter_buf[metadata.bloom_filter_size];
        memset(filter_buf, 0, metadata.bloom_filter_size);
        for (int i = 0; i < metadata.num_entries; i++) {
            key_t key = data[i].key;
            filter.Insert(key, filter_buf);
        }
        // write to file
        wf.write((char*)filter_buf, metadata.bloom_filter_size);
    }
    if (config.sst_type == B_TREE) {
        // create B-tree from memory
        std::vector<key_t> all_keys;
        for (int i = 0; i < metadata.num_entries; i++) {
            key_t key = data[i].key;
            all_keys.push_back(key);
        }
        BPTree::writeBtreeToFile(all_keys, wf);
    }
    
    wf.close();

    if (!wf.good()) {
        std::cout << "Error occurred at writing time!" << std::endl;
    }
    auto new_sst = sstable(filename, cur_version, config);
    return new_sst;
}

bool sstable::Get(key_t key, uint8_t* filter, uint8_t* b_tree_nodes, kv_pair* out_buf) {
    
    if (this->config.bits_per_entry > 0) {
        // query bloom filter first
        BloomFilter bloom_filter(this->file_info.num_entries, this->config.bits_per_entry);
        if (!bloom_filter.Query(key, filter)) {
            // bloom filter returns negative
            // printf("Bloom filter negative\n");
            return false;
        }
    }
    // printf("Get data from SST file %s \n", this->filename.c_str());
    if (this->config.sst_type == B_TREE) {
        // search in b-tree
        int b_tree_size = this->data_size - 
                            (sizeof(sst_metadata_t)
                            + this->file_info.num_entries*sizeof(kv_pair)
                            + this->file_info.bloom_filter_size);
        int page_idx = b_tree_search_page_of_key(b_tree_nodes, b_tree_size, key, false);
        if (page_idx == -1) {
            return false;
        }

        FILE* fptr = fopen(this->filename.c_str(), "rb");
        int read_size = PAGE_SIZE;
        int dataSize = this->file_info.num_entries*sizeof(kv_pair);
        if ((page_idx+1)*PAGE_SIZE > dataSize) {
            // last page might not be of size PAGE_SIZE
            read_size = PAGE_SIZE - ((page_idx+1)*PAGE_SIZE - dataSize);
        }
        pread(fileno(fptr), out_buf, read_size, sizeof(sst_metadata_t) + page_idx*PAGE_SIZE);
        fclose(fptr);
        return true;
    } else {
        FILE* fptr = fopen(this->filename.c_str(), "rb");
        // Binary search of page containing the key in SST
        int page_idx = binary_search_page_of_key(out_buf, key, this->file_info.num_entries*sizeof(kv_pair), fileno(fptr), false);
        fclose(fptr);
        return (page_idx != -1);
    }
  
}

void sstable::Scan(key_t key1, key_t key2, std::vector<kv_pair>& res) {

  FILE* file_ptr;
  char buffer[PAGE_SIZE];
  file_ptr = fopen(this->filename.c_str(), "rb");

  int content_offset = sizeof(sst_metadata_t);

  int dataSize = this->file_info.num_entries*sizeof(kv_pair);
  int off_set = -1;
  int end = -1;
  if (this->config.sst_type == B_TREE) 
  {
    // b-tree search
    int b_tree_size = this->data_size - 
                        (sizeof(sst_metadata_t)
                        + this->file_info.num_entries*sizeof(kv_pair)
                        + this->file_info.bloom_filter_size);
    uint8_t b_tree_nodes[b_tree_size];
    this->GetBTree(b_tree_nodes);
    off_set = b_tree_search_page_of_key(b_tree_nodes, b_tree_size, key1, true);
    end = b_tree_search_page_of_key(b_tree_nodes, b_tree_size, key2, true);
  } 
  else 
  {
    off_set = binary_search_page_of_key((kv_pair*)buffer, key1, dataSize, fileno(file_ptr), true);
    end = binary_search_page_of_key((kv_pair*)buffer, key2, dataSize, fileno(file_ptr), true);
  }

  if (off_set == -1) {
    // if key_1 not found, we need to scan from the beginning
    off_set = 0;
  }
  if (end == -1) {
    // if key_2 not found, need to scan until the end
    end = (dataSize-1)/PAGE_SIZE;
  }

  while (off_set <= end) // to read file
    {
        memset(buffer, TOMBSTONE, PAGE_SIZE);

        int read_size = PAGE_SIZE;
        if ((off_set+1)*PAGE_SIZE > dataSize) {
            // last page might not be of size PAGE_SIZE
            read_size = PAGE_SIZE - ((off_set+1)*PAGE_SIZE - dataSize);
        }
        // function used to read the contents of file
        int bytes_read = pread(fileno(file_ptr), buffer, read_size, content_offset + off_set*PAGE_SIZE);

        for (int i=0; i < bytes_read/sizeof(kv_pair); i++) {
          kv_pair* pair = (kv_pair*) (buffer+ i*sizeof(kv_pair));
          if (key1 <= pair->key && pair->key <= key2) {
            res.push_back(*pair);
          }
        }

      off_set++;
    }
    fclose(file_ptr);
}

void sstable::GetBloomFilter(uint8_t* filter) {
    if (this->config.bits_per_entry > 0) {
        std::ifstream file;
        file.open(this->filename, std::ios::binary);
        file.seekg(sizeof(sst_metadata_t) + this->file_info.num_entries*sizeof(kv_pair));
        file.read((char*)filter, this->file_info.bloom_filter_size);
        file.close();
    }
}

void sstable::GetBTree(uint8_t* internal_nodes) {
    if (this->config.sst_type == B_TREE) {
        std::ifstream file;
        file.open(this->filename, std::ios::binary);
        int offset = sizeof(sst_metadata_t) + this->file_info.num_entries*sizeof(kv_pair) + this->file_info.bloom_filter_size;
        file.seekg(offset);
        file.read((char*)internal_nodes, this->data_size - offset);
        file.close();
    }
}


/* Special helpers for Compaction of LSM Tree */

void sstable::completeSST() {
    std::fstream file;
    file.open(this->filename, std::ios::binary | std::ios::in | std::ios::out);
    sst_metadata_t info;
    file.read((char*)&info, sizeof(sst_metadata_t));

    // for bloom filter:
    uint8_t filter[info.bloom_filter_size];
    memset(filter, 0, info.bloom_filter_size);
    auto bloom = BloomFilter(info.num_entries, this->config.bits_per_entry);
    // for B-tree:
    std::vector<key_t> all_keys;

    for (int i = 0; i < info.num_entries; i++) {
        kv_pair pair;
        file.read((char*)&pair, sizeof(kv_pair));
        // create bloom filter
        if (this->config.bits_per_entry > 0) {
            bloom.Insert(pair.key, filter);
        }
        if (this->config.sst_type == B_TREE) {
            // for B-tree
            all_keys.push_back(pair.key);
        }
    }

    if (this->config.bits_per_entry > 0) {
        // Go to the end of the file to add extra information
        file.seekp(sizeof(sst_metadata_t) + sizeof(kv_pair)*info.num_entries);
        // append bloom filter
        file.write((char*)filter, info.bloom_filter_size);
    }
    if (this->config.sst_type == B_TREE) {
        // Go to the end of the file to add extra information
        file.seekp(sizeof(sst_metadata_t) + sizeof(kv_pair)*info.num_entries + info.bloom_filter_size);
        // create B-tree
        // append B-Tree internal nodes at the end
        BPTree::writeBtreeToFile(all_keys, file);
    }
    file.close();

    // open file
    std::ifstream size_query(this->filename, std::ios::binary);
    // get its size:
    size_query.seekg(0, std::ios::end);
    this->data_size = size_query.tellg();
    size_query.close();

}

// helper function to merge 2 SST into the given output file
sstable sstable::MergeSSTs(sstable new_sst, sstable old_sst, const char* out_filename, int max_lvl) {
    
    kv_pair* in_buf1 = (kv_pair*) malloc(PAGE_SIZE);
    kv_pair* in_buf2 = (kv_pair*) malloc(PAGE_SIZE);
    kv_pair* out_buf = (kv_pair*) malloc(PAGE_SIZE);
    
    FILE* fptr_1 = fopen(new_sst.filename.c_str(), "rb");
    int fd_1 = fileno(fptr_1);
    FILE* fptr_2 = fopen(old_sst.filename.c_str(), "rb");
    int fd_2 = fileno(fptr_2);
    FILE* out_fptr = fopen(out_filename, "wb");
    int out_fd = fileno(out_fptr);
    
    int f1_data_offset = sizeof(sst_metadata_t);
    int f2_data_offset = sizeof(sst_metadata_t);
    int out_data_offset = sizeof(sst_metadata_t);
    sst_metadata_t out_file_info;
    out_file_info.num_entries = 0;

    int bytes_read_1 = pread(fd_1, in_buf1, PAGE_SIZE, f1_data_offset);
    int bytes_read_2 = pread(fd_2, in_buf2, PAGE_SIZE, f2_data_offset);

    int sst1_idx = 0;
    int sst2_idx = 0;
    int out_idx = 0;

    int buf1_idx = 0;
    int buf2_idx = 0;
    int out_buf_idx = 0;

    int sst1_size = new_sst.file_info.num_entries;
    int sst2_size = old_sst.file_info.num_entries;
    
    while (sst1_idx < sst1_size && sst2_idx < sst2_size)
    {
        if (in_buf1[buf1_idx].key == in_buf2[buf2_idx].key) {
            // update to the newest value
            out_buf[out_buf_idx] = in_buf1[buf1_idx];
            sst1_idx++;
            buf1_idx++;
            sst2_idx++;
            buf2_idx++;
        } else if (in_buf1[buf1_idx].key < in_buf2[buf2_idx].key) {
            out_buf[out_buf_idx] = in_buf1[buf1_idx];
            sst1_idx++;
            buf1_idx++;
        } else {
            out_buf[out_buf_idx] = in_buf2[buf2_idx];
            sst2_idx++;
            buf2_idx++;
        }
        // we reached the max level in LSM tree
        if (old_sst.version >= (max_lvl - 1)
            && out_buf[out_buf_idx].value == TOMBSTONE) {
                // discard key with tombstone
                // don't increment index s.t. this entry will be overwritten afterwards
                out_buf[out_buf_idx].key = TOMBSTONE;
        } else {
            // otherwise, increment index to continue writing
            out_buf_idx++;
        }
        
        // read page from storage when we finished with one buffer
        if (buf1_idx == (bytes_read_1 / sizeof(kv_pair))) {
            bytes_read_1 = pread(fd_1, in_buf1, PAGE_SIZE, f1_data_offset + sst1_idx*sizeof(kv_pair));
            buf1_idx = 0;
        }
        if (buf2_idx == (bytes_read_2 / sizeof(kv_pair))) {
            bytes_read_2 = pread(fd_2, in_buf2, PAGE_SIZE, f2_data_offset + sst2_idx*sizeof(kv_pair));
            buf2_idx = 0;
        }
        // flush the page to storage when output buffer is full
        if (out_buf_idx == (PAGE_SIZE / sizeof(kv_pair))) {
            pwrite(out_fd, out_buf, out_buf_idx*sizeof(kv_pair), out_data_offset + out_idx*sizeof(kv_pair));
            out_file_info.num_entries += out_buf_idx;
            out_idx += out_buf_idx;
            out_buf_idx = 0;
        }
    }
    
    // write the rest of the data to file
    while (sst1_idx < sst1_size)
    {
        out_buf[out_buf_idx] = in_buf1[buf1_idx];
        sst1_idx++;
        buf1_idx++;

        // we reached the max level in LSM tree
        if (old_sst.version >= (max_lvl - 1)
            && out_buf[out_buf_idx].value == TOMBSTONE) {
                // discard key with tombstone
                // don't increment index s.t. this entry will be overwritten afterwards
                out_buf[out_buf_idx].key = TOMBSTONE;
        } else {
            // otherwise, increment index to continue writing
            out_buf_idx++;
        }
        
        if (buf1_idx == (bytes_read_1 / sizeof(kv_pair))) {
            bytes_read_1 = pread(fd_1, in_buf1, PAGE_SIZE, f1_data_offset + sst1_idx*sizeof(kv_pair));
            buf1_idx = 0;
        }
        if (out_buf_idx == (PAGE_SIZE / sizeof(kv_pair))) {
            pwrite(out_fd, out_buf, out_buf_idx*sizeof(kv_pair), out_data_offset + out_idx*sizeof(kv_pair));
            out_file_info.num_entries += out_buf_idx;
            out_idx += out_buf_idx;
            out_buf_idx = 0;
        }
    }
    
    while (sst2_idx < sst2_size)
    {
        out_buf[out_buf_idx] = in_buf2[buf2_idx];
        sst2_idx++;
        buf2_idx++;

        // we reached the max level in LSM tree
        if (old_sst.version >= (max_lvl - 1)
            && out_buf[out_buf_idx].value == TOMBSTONE) {
                // discard key with tombstone
                // don't increment index s.t. this entry will be overwritten afterwards
                out_buf[out_buf_idx].key = TOMBSTONE;
        } else {
            // otherwise, increment index to continue writing
            out_buf_idx++;
        }
        
        if (buf2_idx == (bytes_read_2 / sizeof(kv_pair))) {
            bytes_read_2 = pread(fd_2, in_buf2, PAGE_SIZE, f2_data_offset + sst2_idx*sizeof(kv_pair));
            buf2_idx = 0;
        }
        if (out_buf_idx == (PAGE_SIZE / sizeof(kv_pair))) {
            pwrite(out_fd, out_buf, out_buf_idx*sizeof(kv_pair), out_data_offset + out_idx*sizeof(kv_pair));
            out_file_info.num_entries += out_buf_idx;
            out_idx += out_buf_idx;
            out_buf_idx = 0;
        }
    }

    // flush the rest of the data in output buffer
    if (out_buf_idx != 0) {
        pwrite(out_fd, out_buf, out_buf_idx*sizeof(kv_pair), out_data_offset + out_idx*sizeof(kv_pair));
        out_file_info.num_entries += out_buf_idx;
    }

    // update the metadata of the merged file
    if (new_sst.config.bits_per_entry == 0) {
        out_file_info.bloom_filter_size = 0;
    } else {
        out_file_info.bloom_filter_size = (out_file_info.num_entries * new_sst.config.bits_per_entry - 1) / 8 + 1;// in terms of bytes
    }
    pwrite(out_fd, &out_file_info, sizeof(sst_metadata_t), 0);

    // wrap up
    fclose(fptr_1);
    fclose(fptr_2);
    fclose(out_fptr);

    free(in_buf1);
    free(in_buf2);
    free(out_buf);

    auto merged = sstable(out_filename, -2, new_sst.config);
    // Append extra info to the new SST (e.g. bloom filter, b-tree)
    if (merged.config.bits_per_entry > 0 || merged.config.sst_type == B_TREE) {
        merged.completeSST();
    }
    return merged;
}

#include "MapBasedGlobalLockImpl.h"

#include <mutex>

namespace Afina {
namespace Backend {

#define SIZE(key, value) ((key).size() + (value).size())


// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Put(const std::string &key,
                                 const std::string &value)
{
    if (SIZE(key, value) > _max_size) {
        return false;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    if (set(key, value)) {
        return true;
    }
    return putIfAbsent(key, value);
}


// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::PutIfAbsent(const std::string &key,
                                         const std::string &value)
{
    std::lock_guard<std::mutex> lock(_mutex);
    return putIfAbsent(key, value);
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Set(const std::string &key,
                                 const std::string &value)
{
    std::lock_guard<std::mutex> lock(_mutex);
    return set(key, value);
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Delete(const std::string &key)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _backend.find(std::ref(key));
    if (it == _backend.end()) {
        return false;
    }
    size_t deleted_size = SIZE(key, it->second->_key);
    if (it->second == head) {
        head = head->_next;
    }
    if (it->second == tail) {
        tail = tail->_prev;
    }
    _backend.erase(std::ref(it->second->_key));
    delete it->second;
    _storage_size -= deleted_size;
    return true;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Get(const std::string &key,
                                 std::string &value) const
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _backend.find(std::ref(key));
    if (it == _backend.end()) {
        return false;
    }
    if (!refresh_lru(it)) {
        return false;
    }
    value = it->second->_value;
    return true;
}

bool MapBasedGlobalLockImpl::refresh_lru(Map::const_iterator it) const {
    if (it->second != head) {
        it->second->CleanUp();
        head->PutBefore(it->second);
        head = it->second;
    }
    return true;
}

void MapBasedGlobalLockImpl::rotate_cache()
{
    auto it = _backend.find(std::ref(tail->_key));
    while (it == _backend.end() && tail != nullptr){
        // Cleaning  cache, if something not in map somehow.
        it = _backend.find(std::ref(tail->_key));
        tail = tail->_prev;
    }
    if (it != _backend.end()) {
        _storage_size -= SIZE(it->second->_key, it->second->_value);
        _backend.erase(it);
    }
}

bool
MapBasedGlobalLockImpl::set(const std::string &key, const std::string &value) {
    auto it = _backend.find(std::ref(key));
    if (it == _backend.end()) {
        return false;
    }
    refresh_lru(it);
    size_t delta_size = SIZE(key, value) - SIZE(key, it->second->_value);
    if (delta_size > _max_size) {
        return false;
    }
    while (_storage_size + delta_size > _max_size) {
        rotate_cache();
    }
    it->second->_value = value;
    _storage_size +=  delta_size;
    return true;
}

bool MapBasedGlobalLockImpl::putIfAbsent(const std::string &key,
                     const std::string &value) {
    auto it = _backend.find(key);

    if (it == _backend.end()) {
        size_t pair_size = SIZE(key, value);
        if (pair_size > _max_size) {
            return false;
        }
        while (_storage_size + pair_size > _max_size) {
            // Need to rotate
            rotate_cache();
        }
        try {
            // new key-value
            MapEntry *new_entry = new MapEntry(key, value);

            if (head == nullptr && tail == nullptr) {
                head = tail = new_entry;
            } else {
                head->PutBefore(new_entry);
                head = new_entry;
            }

            auto backend_it = _backend.emplace(std::ref(new_entry->_key), new_entry);
            _storage_size += pair_size;
            return true;
        } catch (std::bad_alloc& bad_alloc) {
            return false;
        }

    }
    return false;
}


} // namespace Backend
} // namespace Afina

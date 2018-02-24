#ifndef AFINA_STORAGE_MAP_BASED_GLOBAL_LOCK_IMPL_H
#define AFINA_STORAGE_MAP_BASED_GLOBAL_LOCK_IMPL_H

#include <map>
#include <mutex>
#include <string>

#include <afina/Storage.h>
#include <unordered_map>
#include <list>
#include <assert.h>

namespace Afina {
namespace Backend {

/**
 * # Map based implementation with global lock
 *
 *
 */

class MapBasedGlobalLockImpl : public Afina::Storage {
public:
    MapBasedGlobalLockImpl(size_t max_size = 1024) : _max_size(max_size),
                                                     _storage_size(0),
                                                     head(nullptr), tail(nullptr) {}
    ~MapBasedGlobalLockImpl() {}

    // Implements Afina::Storage interface
    bool Put(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool PutIfAbsent(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool Set(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool Delete(const std::string &key) override;

    // Implements Afina::Storage interface
    bool Get(const std::string &key, std::string &value) const override;

private:
    struct MapEntry {
            const std::string _key;
            std::string _value;

            MapEntry *_prev;
            MapEntry *_next;
            MapEntry(std::string key, std::string value):
                _key(key), _value(value), _prev(nullptr), _next(nullptr){}

            void PutAfter(MapEntry *e) {
                assert(e);
                MapEntry *ex_next = _next;
                _next = e;
                e->_prev = this;
                e->_next = ex_next;
            }

            ~MapEntry() {
                CleanUp();
                _prev = _next = nullptr;
            }

            void PutBefore(MapEntry *e) {
                assert(e);
                MapEntry *ex_prev = _prev;
                _prev = e;
                e->_next = this;
                e->_prev = ex_prev;
            }
            void CleanUp() {
                if (_prev != nullptr) {
                    _prev->_next = _next;
                }
                if (_next != nullptr) {
                    _next->_prev = _prev;
                }
            }

    };
    using Map = std::unordered_map<const std::reference_wrapper<const std::string>,
        MapEntry *,
        std::hash<std::string>,
        std::equal_to<std::string>>;
    Map _backend;

    mutable MapEntry *head;
    mutable MapEntry *tail;

    size_t _max_size;
    size_t _storage_size;
    mutable std::mutex _mutex;
    bool set(const std::string &key, const std::string &value);
    bool putIfAbsent(const std::string &key, const std::string &value);
    bool refresh_lru(Map::const_iterator key) const;
    void rotate_cache();
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_MAP_BASED_GLOBAL_LOCK_IMPL_H

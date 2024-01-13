#include <cerrno>
#include <cstdint>
#include <ostream>
#include <sys/mman.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <future>
#include <iomanip>
#include <ios>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using std::array;
using std::async;
using std::cerr;
using std::cout;
using std::endl;
using std::future;
using std::getline;
using std::ifstream;
using std::invalid_argument;
using std::ios_base;
using std::launch;
using std::make_pair;
using std::map;
using std::max;
using std::min;
using std::numeric_limits;
using std::ostream;
using std::pair;
using std::quoted;
using std::runtime_error;
using std::stod;
using std::string;
using std::string_view;
using std::swap;
using std::thread;
using std::unordered_map;
using std::vector;

namespace {

inline int64_t digit(char c)
{
    if (c < '0' || c > '9') {
        throw invalid_argument(__FUNCTION__);
    }
    return c - '0';
}

inline int64_t non_neg_number(string_view s)
{
    if (s.size() == 3 && s[1] == '.') { // "1.2"
        return digit(s[0]) * 10 + digit(s[2]);
    } else if (s.size() == 4 && s[2] == '.') { // "12.3"
        return digit(s[0]) * 100 + digit(s[1]) * 10 + digit(s[3]);
    } else {
        throw invalid_argument(__FUNCTION__);
    }
}

inline int64_t number(string_view s)
{
    if (!s.empty() && s[0] == '-') {
        return -non_neg_number(s.substr(1));
    } else {
        return non_neg_number(s);
    }
}

inline pair<string_view, int64_t> record(string_view s)
{
    const auto p = s.find_first_of(';');
    return make_pair(s.substr(0, p), number(s.substr(p + 1)));
}

inline pair<string_view, string_view> next_line(string_view s)
{
    if (const auto p = s.find_first_of('\n'); p != string_view::npos) {
        return make_pair(s.substr(0, p), s.substr(p + 1));
    } else {
        return make_pair(s, string_view());
    }
}

//----------------------------------------------------------------------------

struct stats {
    void update(int64_t x)
    {
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x;
        }
        sum += x;
        n++;
    }

    void update(const stats& other)
    {
        if (other.min < min) {
            min = other.min;
        }
        if (other.max > max) {
            max = other.max;
        }
        sum += other.sum;
        n += other.n;
    }

    int64_t min { numeric_limits<int64_t>::max() };
    int64_t max { numeric_limits<int64_t>::min() };
    int64_t sum {};
    size_t n {};
};

ostream& operator<<(ostream& os, const stats& s)
{
    return os << (s.min / 10.0) << '\t' << (s.max / 10.0) << '\t' << (s.sum / 10.0 / s.n);
}

//----------------------------------------------------------------------------

struct file_descr {
    friend struct mmap_file;

    file_descr(const string& path)
        : fd_ { open(path.c_str(), O_RDONLY) }
    {
        if (fd_ == -1) {
            throw runtime_error(strerror(errno));
        }
    }

    ~file_descr()
    {
        if (fd_ != -1 && close(fd_) == -1) {
            cerr << "file_descr: " << strerror(errno) << endl;
        }
    }

    file_descr(const file_descr&) = delete;
    file_descr& operator=(const file_descr&) = delete;

private:
    int fd_ { -1 };
};

//----------------------------------------------------------------------------

struct mmap_file {
    mmap_file(file_descr fd)
    {
        struct stat st;
        if (fstat(fd.fd_, &st) == -1) {
            throw runtime_error(strerror(errno));
        }
        size_ = st.st_size;
        data_ = static_cast<char*>(mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd.fd_, 0));
        if (!data_) {
            throw runtime_error(strerror(errno));
        }
    }

    ~mmap_file()
    {
        if (data_ && munmap(data_, size_) == -1) {
            cerr << "mmap_file: " << strerror(errno) << endl;
        }
    }

    mmap_file(const mmap_file&) = delete;
    mmap_file& operator=(const mmap_file&) = delete;

    operator const string_view() const
    {
        return { data_, size_ };
    }

private:
    char* data_ {};
    size_t size_ {};
};

//----------------------------------------------------------------------------

using stats_table = unordered_map<string_view, stats>;
using stats_table_future = future<stats_table>;

stats_table process_stats(string_view data)
{
    stats_table table;

    // cerr << "process_stats: data " << quoted(data) << ", size " << data.size() << endl;
    // return table;

    for (string_view text { data }; !text.empty();) {
        const auto [line, rest] = next_line(text);
        const auto [name, meas] = record(line);
        table[name].update(meas);
        text = rest;
    }

    return table;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        cerr << "usage: " << argv[0] << " file" << endl;
        return 1;
    }

    mmap_file file { file_descr { argv[1] } };
    string_view text { file };

    const auto n_cpus = thread::hardware_concurrency();
    const auto chunk_size = text.size() / n_cpus;

    stats_table_future results[n_cpus];

    for (size_t i = 0; i < n_cpus - 1; ++i) {
        const auto chunk_end = text.find_first_of('\n', chunk_size) + 1;
        cerr << "i " << i << ", chunk_end " << chunk_end << endl;
        results[i] = async(launch::async, process_stats, text.substr(0, chunk_end));
        text = text.substr(chunk_end);
    }
    results[n_cpus - 1] = async(launch::async, process_stats, text);

    map<string_view, stats> ordered;

    for (auto& r : results) {
        for (const auto& item : r.get()) {
            if (auto it = ordered.find(item.first); it != ordered.end()) {
                it->second.update(item.second);
            } else {
                ordered.insert(item);
            }
        }
    }

    for (const auto& [name, stats] : ordered) {
        cout << name << '\t' << stats << endl;
    }

    return 0;
}

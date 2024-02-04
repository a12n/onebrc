#include <sys/mman.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

using std::async;
using std::cerr;
using std::cout;
using std::endl;
using std::fixed;
using std::future;
using std::invalid_argument;
using std::launch;
using std::make_pair;
using std::map;
using std::max;
using std::min;
using std::numeric_limits;
using std::ostream;
using std::pair;
using std::runtime_error;
using std::setprecision;
using std::string;
using std::string_view;
using std::thread;
using std::unordered_map;

namespace {

//----------------------------------------------------------------------------
// Various parsing functions.

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
    if (const auto i = s.find_first_of(';'); i != string_view::npos) {
        return make_pair(s.substr(0, i), number(s.substr(i + 1)));
    } else {
        throw invalid_argument(__FUNCTION__);
    }
}

inline pair<string_view, string_view> first_line(string_view s)
{
    if (const auto i = s.find_first_of('\n'); i != string_view::npos) {
        return make_pair(s.substr(0, i), s.substr(i + 1));
    } else {
        return make_pair(s, string_view());
    }
}

//----------------------------------------------------------------------------
// Statistics data structure.

struct statistics {
    statistics() = default;

    statistics(int64_t x)
        : min { x }
        , max { x }
        , sum { x }
        , n { 1 }
    {
    }

    void update(const statistics& s)
    {
        if (s.min < min) {
            min = s.min;
        }
        if (s.max > max) {
            max = s.max;
        }
        sum += s.sum;
        n += s.n;
    }

    int64_t min { numeric_limits<int64_t>::max() };
    int64_t max { numeric_limits<int64_t>::min() };
    int64_t sum {};
    size_t n {};
};

ostream& operator<<(ostream& os, const statistics& s)
{
    return os << (s.min / 10.0) << '\t' << (s.sum / 10.0 / s.n) << '\t' << (s.max / 10.0);
}

//----------------------------------------------------------------------------
// Memory-mapped file contents.

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
// Parse and process lines from text.

using ordered_statistics = map<string_view, statistics>;
using unordered_statistics = unordered_map<string_view, statistics>;

unordered_statistics aggregate(string_view input)
{
    unordered_statistics result(1000);

    while (!input.empty()) {
        const auto [line, other_lines] = first_line(input);
        const auto [name, value] = record(line);
        result[name].update(value);
        input = other_lines;
    }

    cerr << "aggregate: load_factor " << result.load_factor() << endl;

    return result;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 2) {
        cerr << "usage: " << argv[0] << " file" << endl;
        return 1;
    }

    mmap_file file { file_descr { argv[1] } };
    string_view input { file };

    const auto n_cpus = thread::hardware_concurrency();
    const auto chunk_size = input.size() / n_cpus;

    future<unordered_statistics> partial[n_cpus];

    for (unsigned i = 0; i < n_cpus - 1; ++i) {
        const auto chunk_end = input.find_first_of('\n', chunk_size) + 1;
        cerr << "Chunk " << (i + 1) << ", size " << chunk_end << endl;
        partial[i] = async(launch::async, aggregate, input.substr(0, chunk_end));
        input = input.substr(chunk_end);
    }
    cerr << "Chunk " << n_cpus << ", size " << input.size() << endl;
    partial[n_cpus - 1] = async(launch::async, aggregate, input);

    ordered_statistics result;

    for (auto& part : partial) {
        for (const auto& item : part.get()) {
            if (auto it = result.find(item.first); it != result.end()) {
                it->second.update(item.second);
            } else {
                result.insert(item);
            }
        }
    }

    cout << fixed << setprecision(1);
    for (const auto& [name, stats] : result) {
        cout << name << '\t' << stats << endl;
    }

    return 0;
}

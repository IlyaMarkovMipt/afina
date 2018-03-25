#include <fcntl.h>
#include "gtest/gtest.h"

#include "afina/Executor.h"

using namespace Afina;

const char *str = "hello world!";

void write_file(int i) {
    char tmpl[] = "/tmp/tmpdir.XXXXXX";
    const char *tmp_dir = mkdtemp(tmpl);
    if (tmp_dir == NULL) {
        throw std::runtime_error("can't create tmp");
    }
    char tmp_filename[30];
    sprintf(tmp_filename, "%s/%i.log", tmp_dir, i);
    int fd = open(tmp_filename, O_WRONLY | O_CREAT);
    if (fd < 0) {
        return;
    }
    int written = 0;
    while (written < sizeof(str)) {
        ssize_t tmp = write(fd, str + written, sizeof(tmp) - written);
        if (tmp < 0) {
            throw std::runtime_error("can't write to file");
        }
    }
}

void set(int *arr, int i) {
    arr[i] = i;
}

TEST(ExecutorTest, Execute) {
    int Q_size = 5;
    Executor executor(Q_size, 3, 1);
    int *arr = new int[Q_size];

    for (int i = 0; i < Q_size; i++) {
        arr[i] = -1;
        EXPECT_TRUE(executor.Execute(set, arr, i));
    }
    for (int i = 0; i < Q_size; i++) {
        while (arr[i] != i) {
            usleep(1);
        }
        EXPECT_EQ(arr[i], i);
    }
}
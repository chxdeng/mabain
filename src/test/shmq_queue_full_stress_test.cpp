/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2.
 */

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <poll.h>
#include <string>
#include <system_error>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "../db.h"
#include "../error.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

constexpr uint32_t kQueueSize = 2;
constexpr int kProducerCount = 16;
constexpr int kRequestsPerProducer = 500;

bool ReadByte(int fd, char& value, int timeout_ms)
{
    struct pollfd pfd = { fd, POLLIN, 0 };
    int rval;
    do {
        rval = poll(&pfd, 1, timeout_ms);
    } while (rval < 0 && errno == EINTR);

    if (rval != 1 || !(pfd.revents & POLLIN))
        return false;

    ssize_t nread;
    do {
        nread = read(fd, &value, sizeof(value));
    } while (nread < 0 && errno == EINTR);
    return nread == static_cast<ssize_t>(sizeof(value));
}

bool WriteByte(int fd, char value)
{
    ssize_t nwritten;
    do {
        nwritten = write(fd, &value, sizeof(value));
    } while (nwritten < 0 && errno == EINTR);
    return nwritten == static_cast<ssize_t>(sizeof(value));
}

bool WaitForChild(pid_t pid, int& status, std::chrono::seconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        pid_t rval = waitpid(pid, &status, WNOHANG);
        if (rval == pid)
            return true;
        if (rval < 0 && errno != EINTR)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

class WriterChild {
public:
    WriterChild(pid_t child_pid, int control_fd)
        : pid(child_pid)
        , fd(control_fd)
        , stopped(false)
    {
    }

    ~WriterChild()
    {
        if (pid <= 0)
            return;
        if (stopped)
            kill(pid, SIGCONT);
        kill(pid, SIGTERM);
        int status = 0;
        waitpid(pid, &status, 0);
        if (fd >= 0)
            close(fd);
    }

    bool Pause()
    {
        if (kill(pid, SIGSTOP) != 0)
            return false;
        int status = 0;
        pid_t rval;
        do {
            rval = waitpid(pid, &status, WUNTRACED);
        } while (rval < 0 && errno == EINTR);
        if (rval != pid || !WIFSTOPPED(status))
            return false;
        stopped = true;
        return true;
    }

    bool Resume()
    {
        if (!stopped)
            return true;
        if (kill(pid, SIGCONT) != 0)
            return false;
        stopped = false;
        return true;
    }

    bool Shutdown()
    {
        if (!Resume() || !WriteByte(fd, 'q'))
            return false;
        close(fd);
        fd = -1;

        int status = 0;
        if (!WaitForChild(pid, status, std::chrono::seconds(10)))
            return false;
        pid = -1;
        return WIFEXITED(status) && WEXITSTATUS(status) == 0;
    }

private:
    pid_t pid;
    int fd;
    bool stopped;
};

int RunWriter(const std::string& db_path, const std::string& queue_dir,
    int ready_fd, int control_fd)
{
    MBConfig config = { 0 };
    config.mbdir = db_path.c_str();
    config.options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
    config.memcap_index = 64 * 1024 * 1024LL;
    config.memcap_data = 64 * 1024 * 1024LL;
    config.queue_size = kQueueSize;
    config.queue_dir = queue_dir.c_str();
    config.async_queue_reservation_timeout_sec = 10;

    DB db(config);
    if (!WriteByte(ready_fd, db.is_open() ? '1' : '0'))
        return 2;
    close(ready_fd);
    if (!db.is_open())
        return 3;

    char command = 0;
    if (!ReadByte(control_fd, command, -1) || command != 'q')
        return 4;
    close(control_fd);
    return db.Close() == MBError::SUCCESS ? 0 : 5;
}

int QueueWithRetry(DB& db, const std::string& key, const std::string& value,
    std::chrono::steady_clock::time_point deadline, std::atomic<uint64_t>* retries)
{
    for (;;) {
        int rval = db.Add(key, value, true);
        if (rval != MBError::TRY_AGAIN)
            return rval;
        if (std::chrono::steady_clock::now() >= deadline)
            return rval;
        if (retries != nullptr)
            retries->fetch_add(1, std::memory_order_relaxed);
        std::this_thread::yield();
    }
}

bool WaitForValue(DB& db, const std::string& key, const std::string& expected,
    std::chrono::seconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        MBData data;
        int rval = db.Find(key, data);
        if (rval == MBError::SUCCESS) {
            return std::string(reinterpret_cast<const char*>(data.buff), data.data_len)
                == expected;
        }
        if (rval != MBError::NOT_EXIST && rval != MBError::TRY_AGAIN)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

bool WaitUntilIdle(DB& db, std::chrono::seconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (!db.AsyncWriterBusy())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

std::string StressKey(int producer, int request)
{
    return "shmq-stress-" + std::to_string(producer) + "-" + std::to_string(request);
}

} // namespace

int main()
{
    signal(SIGPIPE, SIG_IGN);

    std::string db_dir = "/var/tmp/mabain_shmq_full_stress_" + std::to_string(getpid());
    std::string db_path = db_dir + "/";
    std::error_code ec;
    std::filesystem::remove_all(db_dir, ec);
    ec.clear();
    if (!std::filesystem::create_directories(db_dir, ec) || ec) {
        std::cerr << "failed to create test directory: " << ec.message() << std::endl;
        return 1;
    }

    int ready_pipe[2];
    int control_pipe[2];
    if (pipe(ready_pipe) != 0 || pipe(control_pipe) != 0) {
        std::cerr << "failed to create control pipes" << std::endl;
        return 2;
    }

    pid_t writer_pid = fork();
    if (writer_pid < 0) {
        std::cerr << "failed to fork writer" << std::endl;
        return 3;
    }
    if (writer_pid == 0) {
        close(ready_pipe[0]);
        close(control_pipe[1]);
        int rval = RunWriter(db_path, db_dir, ready_pipe[1], control_pipe[0]);
        _exit(rval);
    }

    close(ready_pipe[1]);
    close(control_pipe[0]);
    WriterChild writer(writer_pid, control_pipe[1]);

    char ready = 0;
    if (!ReadByte(ready_pipe[0], ready, 10000) || ready != '1') {
        std::cerr << "writer failed to initialize" << std::endl;
        return 4;
    }
    close(ready_pipe[0]);

    MBConfig producer_config = { 0 };
    producer_config.mbdir = db_path.c_str();
    producer_config.options = CONSTS::ReaderOptions();
    producer_config.memcap_index = 64 * 1024 * 1024LL;
    producer_config.memcap_data = 64 * 1024 * 1024LL;
    producer_config.queue_size = kQueueSize;
    producer_config.queue_dir = db_dir.c_str();
    DB monitor_db(producer_config);
    if (!monitor_db.is_open()) {
        std::cerr << "producer failed to open database: " << monitor_db.StatusStr() << std::endl;
        return 5;
    }

    // Stop the writer so the two-slot queue can be filled deterministically.
    if (!writer.Pause()) {
        std::cerr << "failed to pause writer" << std::endl;
        return 6;
    }

    const std::string prefill_value = "prefill-value";
    for (uint32_t i = 0; i < kQueueSize; ++i) {
        std::string key = "shmq-prefill-" + std::to_string(i);
        int rval = monitor_db.Add(key, prefill_value, true);
        if (rval != MBError::SUCCESS) {
            std::cerr << "failed to fill queue at slot " << i << std::endl;
            return 7;
        }
    }

    const std::string rejected_key = "shmq-must-retry";
    if (monitor_db.Add(rejected_key, prefill_value, true) != MBError::TRY_AGAIN) {
        std::cerr << "full queue did not reject acquisition" << std::endl;
        return 8;
    }

    if (!writer.Resume()) {
        std::cerr << "failed to resume writer" << std::endl;
        return 9;
    }
    for (uint32_t i = 0; i < kQueueSize; ++i) {
        std::string key = "shmq-prefill-" + std::to_string(i);
        if (!WaitForValue(monitor_db, key, prefill_value, std::chrono::seconds(5))) {
            std::cerr << "writer failed to process prefilled request " << i << std::endl;
            return 10;
        }
    }

    // A successful request queued after the rejected acquisition must not sit
    // behind a logical hole for the writer's ten-second reservation timeout.
    const std::string sentinel_key = "shmq-after-rejection";
    auto sentinel_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    if (QueueWithRetry(monitor_db, sentinel_key, prefill_value,
            sentinel_deadline, nullptr)
        != MBError::SUCCESS) {
        std::cerr << "failed to queue sentinel request" << std::endl;
        return 11;
    }
    if (!WaitForValue(monitor_db, sentinel_key, prefill_value,
            std::chrono::seconds(5))) {
        std::cerr << "sentinel request was blocked behind a queue hole" << std::endl;
        return 12;
    }

    std::atomic<bool> start(false);
    std::atomic<int> ready_count(0);
    std::atomic<int> failures(0);
    std::atomic<uint64_t> retries(0);

    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (int producer = 0; producer < kProducerCount; ++producer) {
        producers.emplace_back([&, producer]() {
            MBConfig config = producer_config;
            DB db(config);
            bool open = db.is_open();
            ready_count.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            if (!open) {
                failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
            const std::string value(512, static_cast<char>('a' + producer % 26));
            for (int request = 0; request < kRequestsPerProducer; ++request) {
                std::string key = StressKey(producer, request);
                if (QueueWithRetry(db, key, value, deadline, &retries)
                    != MBError::SUCCESS) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }

    while (ready_count.load(std::memory_order_acquire) != kProducerCount)
        std::this_thread::yield();
    start.store(true, std::memory_order_release);

    for (std::thread& producer : producers)
        producer.join();

    if (failures.load(std::memory_order_acquire) != 0) {
        std::cerr << "producer stress failed" << std::endl;
        return 13;
    }
    if (retries.load(std::memory_order_acquire) == 0) {
        std::cerr << "stress did not create queue contention" << std::endl;
        return 14;
    }
    if (!WaitUntilIdle(monitor_db, std::chrono::seconds(30))) {
        std::cerr << "writer failed to drain stress requests" << std::endl;
        return 15;
    }

    for (int producer = 0; producer < kProducerCount; ++producer) {
        const std::string expected(512, static_cast<char>('a' + producer % 26));
        for (int request = 0; request < kRequestsPerProducer; ++request) {
            std::string key = StressKey(producer, request);
            MBData data;
            int rval = monitor_db.Find(key, data);
            if (rval != MBError::SUCCESS
                || std::string(reinterpret_cast<const char*>(data.buff), data.data_len)
                    != expected) {
                std::cerr << "missing or corrupt stress request: " << key << std::endl;
                return 16;
            }
        }
    }

    monitor_db.Close();
    ResourcePool::getInstance().RemoveAll();
    if (!writer.Shutdown()) {
        std::cerr << "writer failed to shut down cleanly" << std::endl;
        return 17;
    }

    std::filesystem::remove_all(db_dir, ec);
    if (ec) {
        std::cerr << "test passed, but cleanup failed: " << ec.message() << std::endl;
        return 18;
    }

    std::cout << "shmq_queue_full_stress_test passed with "
              << retries.load(std::memory_order_relaxed) << " full-queue retries" << std::endl;
    return 0;
}

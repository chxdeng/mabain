/**
 * Test to replicate Mabain's actual use case under disk pressure
 *
 * PURPOSE:
 * This test demonstrates the critical difference between ftruncate() and fallocate()
 * for memory-mapped database files under extreme disk pressure conditions.
 *
 * THE PROBLEM:
 * - ftruncate() creates "sparse files" - file size is set but disk space isn't allocated
 * - When database accesses memory-mapped sparse regions under disk pressure, SIGBUS occurs
 * - This causes unexpected database crashes in production environments
 *
 * THE SOLUTION:
 * - fallocate() ensures real disk space allocation upfront
 * - Database operations either succeed completely or fail gracefully during initialization
 * - No surprise SIGBUS crashes during normal database operations
 *
 * TEST STRATEGY:
 * 1. Create database files using current allocation method (TruncateFile vs AllocateFile)
 * 2. Fill disk to 100% capacity in separate process (process isolation prevents interference)
 * 3. Perform database insertions that require file growth
 * 4. Monitor for SIGBUS during memory-mapped access to sparse file regions
 * 5. Validate that fallocate() prevents SIGBUS while ftruncate() causes it
 */

#include "../db.h"
#include "../mabain_consts.h"
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <random>
#include <signal.h>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

using namespace mabain;

// Global flag to track if SIGBUS occurred (only for Mabain operations)
// This flag is only set by our custom SIGBUS handler, not by the disk-filling process
volatile sig_atomic_t bus_error_occurred = 0;
volatile int bus_error_count = 0;

// Global pointer to database for SIGBUS handler access
// Allows emergency database diagnostics when SIGBUS occurs
DB* global_db_ptr = nullptr;

void sigbus_handler(int sig, siginfo_t* si, void* unused)
{
    // CRITICAL: This handler captures SIGBUS during memory-mapped database access
    // SIGBUS occurs when accessing sparse file regions with no available disk space
    // This demonstrates the core ftruncate() vs fallocate() problem

    bus_error_occurred = 1;
    bus_error_count++;
    std::cout << "\n*** MABAIN SIGBUS #" << bus_error_count << " CAUGHT! ***" << std::endl;
    std::cout << "Mabain database operation failed due to sparse file allocation!" << std::endl;
    std::cout << "Address: " << si->si_addr << std::endl;
    std::cout << "This demonstrates the danger of ftruncate() vs fallocate() for database files." << std::endl;
    std::cout << "SIGBUS occurred during memory-mapped database access." << std::endl;

    // *** COMPREHENSIVE MABAIN DATABASE DUMP BEFORE EXIT ***
    // When SIGBUS occurs, capture complete database state for debugging
    // This helps understand exactly what operation triggered the sparse file access
    std::cout << "\n=== EMERGENCY MABAIN DATABASE DUMP (SIGBUS STATE) ===" << std::endl;

    if (global_db_ptr != nullptr) {
        try {
            std::cout << "Database pointer: " << global_db_ptr << std::endl;
            std::cout << "Database is_open(): " << (global_db_ptr->is_open() ? "TRUE" : "FALSE") << std::endl;
            std::cout << "Database StatusStr(): " << global_db_ptr->StatusStr() << std::endl;

            // Try to get any database statistics or state information
            std::cout << "\n--- Attempting Database State Analysis ---" << std::endl;

            // Try a simple operation to see what fails
            std::cout << "Testing basic database operations at SIGBUS..." << std::endl;

            // Test key lookup that was working before
            MBData test_mbd;
            std::cout << "Attempting lookup of initial_test_key..." << std::endl;
            int lookup_result = global_db_ptr->Find("initial_test_key", test_mbd);
            std::cout << "Lookup result: " << lookup_result << std::endl;

            if (lookup_result == 0) {
                std::string value(reinterpret_cast<const char*>(test_mbd.buff), test_mbd.data_len);
                std::cout << "Successfully retrieved: " << value << std::endl;
            } else {
                std::cout << "Lookup failed with error: " << lookup_result << std::endl;
            }

        } catch (const std::exception& e) {
            std::cout << "Exception during SIGBUS database dump: " << e.what() << std::endl;
        } catch (...) {
            std::cout << "Unknown exception during SIGBUS database dump" << std::endl;
        }
    } else {
        std::cout << "ERROR: Global database pointer is NULL!" << std::endl;
    }

    // Check database files at the moment of SIGBUS
    // This analysis shows which files are sparse and caused the SIGBUS
    std::cout << "\n--- Database Files State at SIGBUS ---" << std::endl;
    const std::string db_dir = "/tmp/mabain_test_db/";
    std::vector<std::string> db_files = {
        db_dir + "_mabain_i", // Index file
        db_dir + "_mabain_d", // Data file
        db_dir + "_mabain_l" // Lock file
    };

    for (const auto& file : db_files) {
        struct stat file_stat;
        if (stat(file.c_str(), &file_stat) == 0) {
            std::cout << "SIGBUS File: " << file << std::endl;
            std::cout << "  Size: " << file_stat.st_size << " bytes" << std::endl;
            std::cout << "  Blocks: " << file_stat.st_blocks << " (512-byte blocks)" << std::endl;
            std::cout << "  Allocated: " << (file_stat.st_blocks * 512) << " bytes" << std::endl;

            // CRITICAL CHECK: Detect sparse files that cause SIGBUS
            // If allocated bytes < file size, it's a sparse file created by ftruncate()
            if (static_cast<size_t>(file_stat.st_blocks * 512) < static_cast<size_t>(file_stat.st_size)) {
                std::cout << "  *** SPARSE FILE - THIS IS THE PROBLEM! ***" << std::endl;
                std::cout << "  Sparse gap: " << (static_cast<size_t>(file_stat.st_size) - static_cast<size_t>(file_stat.st_blocks * 512)) << " bytes" << std::endl;
                std::cout << "  This sparse region caused SIGBUS when accessed under disk pressure!" << std::endl;
            } else {
                std::cout << "  Fully allocated file" << std::endl;
            }
        } else {
            std::cout << "SIGBUS File: " << file << " (not accessible)" << std::endl;
        }
    }

    // Check current disk space at moment of SIGBUS
    std::cout << "\n--- Disk Space at SIGBUS ---" << std::endl;
    struct statvfs disk_stat;
    if (statvfs("/tmp", &disk_stat) == 0) {
        size_t free_kb = (disk_stat.f_bavail * disk_stat.f_frsize) / 1024;
        size_t total_mb = (disk_stat.f_blocks * disk_stat.f_frsize) / (1024 * 1024);
        size_t free_mb = free_kb / 1024;
        double usage_percent = 100.0 * (total_mb - free_mb) / total_mb;

        std::cout << "Available space: " << free_kb << " KB (" << free_mb << " MB)" << std::endl;
        std::cout << "Disk usage: " << std::fixed << std::setprecision(2) << usage_percent << "%" << std::endl;

        if (usage_percent >= 99.0) {
            std::cout << "*** CONFIRMED: Disk at 100% - SIGBUS due to sparse file access ***" << std::endl;
        }
    } else {
        std::cout << "Cannot get disk statistics at SIGBUS" << std::endl;
    }

    std::cout << "\n=== END EMERGENCY DATABASE DUMP ===" << std::endl;
    std::cout << "Exiting immediately to prevent further SIGBUS signals." << std::endl;

    // Exit immediately after comprehensive dump
    exit(1);
}

// Create aggressive disk pressure in a separate process
// PROCESS ISOLATION: Critical for accurate SIGBUS testing
// - Child process fills disk but has SIGBUS handler reset to default
// - Parent process monitors only Mabain operations for SIGBUS
// - Prevents false positives from disk-filling operations
pid_t create_disk_pressure_process()
{
    struct statvfs stat;
    if (statvfs("/tmp", &stat) != 0) {
        std::cerr << "Failed to get filesystem stats" << std::endl;
        return -1;
    }

    size_t free_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024);
    std::cout << "Available disk space: " << free_mb << " MB" << std::endl;

    if (free_mb < 100) {
        std::cout << "Not enough free space to create disk pressure safely" << std::endl;
        return -1;
    }

    pid_t pid = fork();
    if (pid == 0) {
        // Child process: reset SIGBUS handler to default (don't interfere with parent's counting)
        // CRITICAL: This prevents the disk-filling process from triggering our SIGBUS handler
        // Only the parent process (running Mabain operations) should catch SIGBUS
        signal(SIGBUS, SIG_DFL);

        // Child process: fill disk space to 100% capacity
        // GOAL: Create true disk pressure to trigger SIGBUS on sparse file access
        // Strategy: Fill disk leaving minimal space, then fill even that
        std::cout << "[DISK FILLER] Starting MAXIMUM disk pressure creation (targeting 100%)..." << std::endl;
        std::cout << "[DISK FILLER] SIGBUS handler reset to default for disk filling process" << std::endl;

        // Fill disk leaving only minimal space (true 100% pressure)
        size_t fill_mb = free_mb - 1; // Leave only 1MB, then fill even that
        std::cout << "[DISK FILLER] Creating " << fill_mb << " MB of temporary files..." << std::endl;

        const size_t file_size_mb = 50; // Smaller files for more precise control
        size_t files_needed = (fill_mb + file_size_mb - 1) / file_size_mb;

        std::vector<char> buffer(file_size_mb * 1024 * 1024, 'X');
        std::vector<std::string> temp_files;

        // Phase 1: Fill most of the space with large files
        // Use predictable file sizes for precise disk pressure control
        for (size_t i = 0; i < files_needed; i++) {
            std::stringstream ss;
            ss << "/tmp/disk_pressure_" << getpid() << "_" << i << ".tmp";
            std::string filename = ss.str();

            int fd = open(filename.c_str(), O_CREAT | O_WRONLY, 0644);
            if (fd < 0) {
                std::cout << "[DISK FILLER] Failed to create pressure file " << filename << std::endl;
                break;
            }

            size_t remaining_mb = (i == files_needed - 1) ? (fill_mb % file_size_mb) : file_size_mb;
            if (remaining_mb == 0)
                remaining_mb = file_size_mb;

            ssize_t written = write(fd, buffer.data(), remaining_mb * 1024 * 1024);
            close(fd);

            if (written <= 0) {
                std::cout << "[DISK FILLER] Disk full at file " << i << "!" << std::endl;
                unlink(filename.c_str());
                break;
            }

            temp_files.push_back(filename);

            if (i % 5 == 0) {
                // Check current disk usage more frequently
                struct statvfs current_stat;
                if (statvfs("/tmp", &current_stat) == 0) {
                    size_t current_free_mb = (current_stat.f_bavail * current_stat.f_frsize) / (1024 * 1024);
                    std::cout << "[DISK FILLER] Created " << (i + 1) << "/" << files_needed
                              << " files, " << current_free_mb << " MB remaining..." << std::endl;
                }
            }
        }

        // Phase 2: Fill remaining space with progressively smaller chunks to reach 100%
        // CRITICAL: Must achieve true 100% disk usage to trigger SIGBUS on sparse files
        std::cout << "[DISK FILLER] Phase 2: Filling remaining space to achieve 100%..." << std::endl;

        struct statvfs remaining_stat;
        if (statvfs("/tmp", &remaining_stat) == 0) {
            size_t remaining_kb = (remaining_stat.f_bavail * remaining_stat.f_frsize) / 1024;
            std::cout << "[DISK FILLER] Remaining space: " << remaining_kb << " KB" << std::endl;

            // Fill remaining space in 100KB chunks
            size_t chunk_count = 0;
            while (remaining_kb > 100) {
                std::stringstream ss;
                ss << "/tmp/disk_final_" << getpid() << "_" << chunk_count << ".tmp";
                std::string filename = ss.str();

                int fd = open(filename.c_str(), O_CREAT | O_WRONLY, 0644);
                if (fd >= 0) {
                    size_t chunk_size = std::min(remaining_kb, (size_t)100) * 1024;
                    std::vector<char> small_buffer(chunk_size, 'Y');
                    ssize_t written = write(fd, small_buffer.data(), chunk_size);
                    close(fd);

                    if (written > 0) {
                        temp_files.push_back(filename);
                        chunk_count++;
                    } else {
                        unlink(filename.c_str());
                        break;
                    }
                }

                // Check remaining space
                if (statvfs("/tmp", &remaining_stat) == 0) {
                    remaining_kb = (remaining_stat.f_bavail * remaining_stat.f_frsize) / 1024;
                } else {
                    break;
                }

                if (chunk_count % 10 == 0) {
                    std::cout << "[DISK FILLER] Final phase: " << remaining_kb << " KB remaining..." << std::endl;
                }
            }

            // Phase 3: Fill the very last bits in 1KB chunks
            std::cout << "[DISK FILLER] Phase 3: Filling final " << remaining_kb << " KB..." << std::endl;
            size_t final_count = 0;
            while (remaining_kb > 1) {
                std::stringstream ss;
                ss << "/tmp/disk_micro_" << getpid() << "_" << final_count << ".tmp";
                std::string filename = ss.str();

                int fd = open(filename.c_str(), O_CREAT | O_WRONLY, 0644);
                if (fd >= 0) {
                    std::vector<char> tiny_buffer(1024, 'Z');
                    ssize_t written = write(fd, tiny_buffer.data(), 1024);
                    close(fd);

                    if (written > 0) {
                        temp_files.push_back(filename);
                        final_count++;
                    } else {
                        unlink(filename.c_str());
                        break;
                    }
                } else {
                    break;
                }

                // Check remaining space
                if (statvfs("/tmp", &remaining_stat) == 0) {
                    remaining_kb = (remaining_stat.f_bavail * remaining_stat.f_frsize) / 1024;
                } else {
                    break;
                }

                // Safety limit - don't create more than 1000 tiny files
                if (final_count > 1000) {
                    std::cout << "[DISK FILLER] Safety limit reached on tiny files" << std::endl;
                    break;
                }
            }
        }

        // Check final available space
        if (statvfs("/tmp", &stat) == 0) {
            size_t final_free_kb = (stat.f_bavail * stat.f_frsize) / 1024;
            size_t final_free_mb = final_free_kb / 1024;
            size_t total_mb = (stat.f_blocks * stat.f_frsize) / (1024 * 1024);
            double usage_percent = 100.0 * (total_mb - final_free_mb) / total_mb;

            std::cout << "[DISK FILLER] FINAL DISK STATE:" << std::endl;
            std::cout << "[DISK FILLER]   Available space: " << final_free_kb << " KB (" << final_free_mb << " MB)" << std::endl;
            std::cout << "[DISK FILLER]   Disk usage: " << std::fixed << std::setprecision(2) << usage_percent << "%" << std::endl;

            if (usage_percent >= 99.0) {
                std::cout << "[DISK FILLER] ✓ Successfully achieved 100% disk pressure!" << std::endl;
                std::cout << "[DISK FILLER] ✓ This should trigger SIGBUS when Mabain accesses sparse file regions!" << std::endl;
            } else {
                std::cout << "[DISK FILLER] ⚠ Only reached " << usage_percent << "% (may not trigger SIGBUS)" << std::endl;
                std::cout << "[DISK FILLER] ⚠ SIGBUS requires true 100% disk pressure for sparse file access!" << std::endl;
            }
        }

        // Keep the files around and wait for parent signal
        std::cout << "[DISK FILLER] Disk pressure created. Waiting for parent..." << std::endl;
        pause(); // Wait for signal from parent

        // Clean up when signaled
        std::cout << "[DISK FILLER] Cleaning up pressure files..." << std::endl;
        for (const std::string& file : temp_files) {
            unlink(file.c_str());
        }

        std::cout << "[DISK FILLER] Cleanup complete. Exiting." << std::endl;
        exit(0);
    } else if (pid > 0) {
        // Parent process: wait for child to create significant pressure
        // MONITORING: Ensure disk reaches 100% before proceeding with Mabain test
        std::cout << "Waiting for disk pressure process to establish critical disk usage..." << std::endl;

        // Wait and monitor disk space
        for (int i = 0; i < 30; i++) {
            sleep(2);
            struct statvfs current_stat;
            if (statvfs("/tmp", &current_stat) == 0) {
                size_t current_free_mb = (current_stat.f_bavail * current_stat.f_frsize) / (1024 * 1024);
                std::cout << "Current available space: " << current_free_mb << " MB" << std::endl;

                // Proceed when disk is at 100% capacity (less than 1MB free)
                // CRITICAL THRESHOLD: SIGBUS only occurs with true disk pressure
                if (current_free_mb < 1) {
                    std::cout << "TRUE 100% disk pressure achieved! Proceeding with Mabain test..." << std::endl;
                    std::cout << "Now any access to sparse file regions should trigger SIGBUS!" << std::endl;
                    return pid;
                }
            }
        }

        std::cout << "Proceeding with test (disk pressure may not be optimal)..." << std::endl;
        return pid;
    } else {
        std::cerr << "Failed to fork disk pressure process" << std::endl;
        return -1;
    }
}

// Clean up disk pressure process
void cleanup_disk_pressure_process(pid_t pressure_pid)
{
    if (pressure_pid > 0) {
        std::cout << "Signaling disk pressure process to clean up..." << std::endl;
        kill(pressure_pid, SIGTERM);
        int status;
        waitpid(pressure_pid, &status, 0);
        std::cout << "Disk pressure process cleaned up." << std::endl;
    }
}

// Test scenario: Mabain insertions only (database already open, disk at 100%)
// CORE TEST: This function performs the actual SIGBUS demonstration
// - Database files already created with current allocation method (TruncateFile vs AllocateFile)
// - Disk is at 100% pressure
// - Insertions force database file growth
// - Memory-mapped access to sparse regions triggers SIGBUS (with ftruncate)
// - fallocate prevents SIGBUS by ensuring real space allocation
void test_mabain_insertions_only(DB* db, size_t index_cap, size_t data_cap)
{
    std::cout << "\n=== Testing 100K Insertions with 100MB Mabain Capacity ===" << std::endl;
    std::cout << "Database is already open with files created by current allocation method" << std::endl;
    std::cout << "Testing whether 100MB capacity can handle 100,000 key-value pairs" << std::endl;
    std::cout << "If SIGBUS occurs with ftruncate(), it indicates sparse file issues" << std::endl;

    // Print comprehensive database state before insertions
    // BASELINE: Establish database health before stress testing
    std::cout << "\n--- Pre-Insertion Database Analysis ---" << std::endl;
    std::cout << "Database status: " << db->StatusStr() << std::endl;
    std::cout << "Database is open: " << (db->is_open() ? "YES" : "NO") << std::endl;

    // Try to get current database statistics
    std::cout << "\n--- Current Database Content Analysis ---" << std::endl;
    try {
        // Test if we can iterate through existing data
        std::cout << "Attempting to iterate through existing database entries..." << std::endl;

        // Try a simple lookup first
        MBData test_mbd;
        int test_lookup = db->Find("initial_test_key", test_mbd);
        std::cout << "Test lookup for initial key: " << test_lookup << std::endl;
        if (test_lookup == 0) {
            std::string test_value(reinterpret_cast<const char*>(test_mbd.buff), test_mbd.data_len);
            std::cout << "Found initial test value: " << test_value << std::endl;
        }

    } catch (const std::exception& e) {
        std::cout << "Exception during database analysis: " << e.what() << std::endl;
    } catch (...) {
        std::cout << "Unknown exception during database analysis" << std::endl;
    }

    bus_error_occurred = 0;
    bus_error_count = 0;

    // First, check initial file sizes to confirm they are sparse
    // SPARSE FILE DETECTION: Key evidence for ftruncate() vs fallocate() behavior
    const std::string db_dir = "/tmp/mabain_test_db/";
    std::string index_file = db_dir + "_mabain_i";
    std::string data_file = db_dir + "_mabain_d";

    std::cout << "\n--- Initial File State Analysis ---" << std::endl;
    struct stat initial_index_stat, initial_data_stat;
    if (stat(index_file.c_str(), &initial_index_stat) == 0) {
        size_t allocated_bytes = initial_index_stat.st_blocks * 512;
        size_t file_size = static_cast<size_t>(initial_index_stat.st_size);
        std::cout << "Index file: size=" << file_size << ", allocated=" << allocated_bytes;
        if (allocated_bytes < file_size) {
            std::cout << " *** SPARSE FILE CONFIRMED ***" << std::endl;
            std::cout << "    This sparse file will cause SIGBUS under disk pressure!" << std::endl;
        } else {
            std::cout << " (fully allocated)" << std::endl;
            std::cout << "    This file has real disk space - no SIGBUS risk" << std::endl;
        }
    }
    if (stat(data_file.c_str(), &initial_data_stat) == 0) {
        size_t allocated_bytes = initial_data_stat.st_blocks * 512;
        size_t file_size = static_cast<size_t>(initial_data_stat.st_size);
        std::cout << "Data file: size=" << file_size << ", allocated=" << allocated_bytes;
        if (allocated_bytes < file_size) {
            std::cout << " *** SPARSE FILE CONFIRMED ***" << std::endl;
            std::cout << "    This sparse file will cause SIGBUS under disk pressure!" << std::endl;
        } else {
            std::cout << " (fully allocated)" << std::endl;
            std::cout << "    This file has real disk space - no SIGBUS risk" << std::endl;
        }
    }

    // Perform many insertions to trigger database file growth
    // INSERTION STRATEGY: Force database operations that require file expansion
    // - Large number of records to exceed initial capacity
    // - Variable key-value sizes to trigger different growth patterns
    // - Monitor file growth and sparse region access
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(1000000, 9999999);

    const int max_insertions = 100000; // Large number to test capacity handling
    int successful_insertions = 0;

    std::cout << "Starting insertions to test 100K pairs with 100MB capacity..." << std::endl;
    std::cout << "Memory cap: " << (index_cap / 1024 / 1024) << "MB index + " << (data_cap / 1024 / 1024) << "MB data = " << ((index_cap + data_cap) / 1024 / 1024) << "MB total" << std::endl;
    std::cout << "Will insert " << max_insertions << " records to test database capacity..." << std::endl;

    for (int i = 0; i < max_insertions && !bus_error_occurred; i++) {
        // Generate reasonable key-value pairs for capacity testing
        // KEY INSIGHT: Each insertion may trigger memory-mapped file access
        // If files are sparse (ftruncate), access under disk pressure = SIGBUS
        std::stringstream key_ss, value_ss;
        key_ss << "test_key_" << i;
        value_ss << "test_value_" << i << "_with_some_data_to_test_capacity";

        std::string key = key_ss.str();
        std::string value = value_ss.str();

        try {
            // CRITICAL OPERATION: Database insertion that may access sparse file regions
            // This is where SIGBUS occurs with ftruncate() under disk pressure
            int result = db->Add(key, value);
            if (result == 0) {
                successful_insertions++;
                if (i % 1000 == 0) { // Report every 1000 insertions
                    std::cout << "Inserted " << i << " records (testing " << ((index_cap + data_cap) / 1024 / 1024) << "MB capacity)..." << std::endl;

                    // Check file sizes every 1000 insertions to monitor growth
                    // GROWTH MONITORING: Track file expansion and sparse region detection
                    const std::string db_dir = "/tmp/mabain_test_db/";
                    std::string index_file = db_dir + "_mabain_i";
                    std::string data_file = db_dir + "_mabain_d";

                    struct stat index_stat, data_stat;
                    if (stat(index_file.c_str(), &index_stat) == 0) {
                        std::cout << "  Index file now: " << index_stat.st_size << " bytes (allocated: " << (index_stat.st_blocks * 512) << ")" << std::endl;
                        if (static_cast<size_t>(index_stat.st_blocks * 512) < static_cast<size_t>(index_stat.st_size)) {
                            std::cout << "  *** Index file is SPARSE - potential SIGBUS target! ***" << std::endl;
                        }
                    }
                    if (stat(data_file.c_str(), &data_stat) == 0) {
                        std::cout << "  Data file now: " << data_stat.st_size << " bytes (allocated: " << (data_stat.st_blocks * 512) << ")" << std::endl;
                        if (static_cast<size_t>(data_stat.st_blocks * 512) < static_cast<size_t>(data_stat.st_size)) {
                            std::cout << "  *** Data file is SPARSE - potential SIGBUS target! ***" << std::endl;
                        }
                    }

                    // Show progress toward 100MB capacity
                    // CAPACITY ANALYSIS: Monitor how much database space is being used
                    size_t total_used = static_cast<size_t>(index_stat.st_size) + static_cast<size_t>(data_stat.st_size);
                    double capacity_used_percent = 100.0 * total_used / (index_cap + data_cap);
                    std::cout << "  Capacity used: " << (total_used / 1024 / 1024) << "MB / " << ((index_cap + data_cap) / 1024 / 1024) << "MB (" << std::fixed << std::setprecision(1) << capacity_used_percent << "%)" << std::endl;
                }
            } else {
                std::cout << "Insert failed at record " << i << ", error: " << result << std::endl;
                if (result == -1) {
                    std::cout << "This could indicate disk space issues!" << std::endl;
                }
                std::cout << "Continuing with more aggressive insertions..." << std::endl;
                // Don't break immediately - keep trying to trigger SIGBUS
            }
        } catch (...) {
            std::cout << "Exception during insertion at record " << i << std::endl;
            std::cout << "Continuing with more insertions to trigger SIGBUS..." << std::endl;
            // Don't break - keep pushing
        }

        // Normal delay for 100K test
        if (i % 1000 == 0) {
            usleep(1000); // 1ms delay every 1000 insertions
        }
    }

    std::cout << "Successfully inserted " << successful_insertions << " records out of " << max_insertions << " attempted" << std::endl;

    // If we completed all insertions successfully, show success stats
    if (!bus_error_occurred && successful_insertions == max_insertions) {
        std::cout << "\n*** SUCCESS: Inserted all " << max_insertions << " records with 100MB capacity! ***" << std::endl;

        // Final capacity usage analysis
        const std::string db_dir = "/tmp/mabain_test_db/";
        std::string index_file = db_dir + "_mabain_i";
        std::string data_file = db_dir + "_mabain_d";

        struct stat final_index_stat, final_data_stat;
        if (stat(index_file.c_str(), &final_index_stat) == 0 && stat(data_file.c_str(), &final_data_stat) == 0) {
            size_t total_used = static_cast<size_t>(final_index_stat.st_size) + static_cast<size_t>(final_data_stat.st_size);
            double capacity_used_percent = 100.0 * total_used / (index_cap + data_cap);

            std::cout << "Final database size: " << (total_used / 1024 / 1024) << "MB / " << ((index_cap + data_cap) / 1024 / 1024) << "MB capacity" << std::endl;
            std::cout << "Capacity utilization: " << std::fixed << std::setprecision(1) << capacity_used_percent << "%" << std::endl;
            std::cout << "Average per record: " << (total_used / max_insertions) << " bytes" << std::endl;
        }
    }
    // If we haven't hit SIGBUS yet, try even more aggressive operations (only if partial insertions)
    else if (!bus_error_occurred && successful_insertions < max_insertions) {
        std::cout << "\n*** NO SIGBUS YET - TRYING EVEN MORE AGGRESSIVE OPERATIONS ***" << std::endl;
        std::cout << "Performing direct memory-mapped file access to force SIGBUS..." << std::endl;

        // Try to force direct access to sparse file regions
        const std::string db_dir = "/tmp/mabain_test_db/";
        std::string index_file = db_dir + "_mabain_i";
        std::string data_file = db_dir + "_mabain_d";

        struct stat final_index_stat, final_data_stat;
        if (stat(index_file.c_str(), &final_index_stat) == 0 && stat(data_file.c_str(), &final_data_stat) == 0) {
            std::cout << "Final file sizes:" << std::endl;
            std::cout << "  Index: " << final_index_stat.st_size << " bytes (allocated: " << (final_index_stat.st_blocks * 512) << ")" << std::endl;
            std::cout << "  Data: " << final_data_stat.st_size << " bytes (allocated: " << (final_data_stat.st_blocks * 512) << ")" << std::endl;

            // Try more insertions with even larger data to force file growth
            std::cout << "Trying insertions with MASSIVE data to force SIGBUS..." << std::endl;
            for (int i = 0; i < 1000 && !bus_error_occurred; i++) {
                std::stringstream massive_key, massive_value;
                massive_key << "massive_key_" << i << "_" << std::string(100, 'K'); // 100 extra chars
                massive_value << "massive_value_" << i << "_" << std::string(1000, 'V'); // 1000 extra chars

                try {
                    int result = db->Add(massive_key.str(), massive_value.str());
                    if (result != 0 && i % 100 == 0) {
                        std::cout << "Massive insert " << i << " failed with error " << result << " - continuing..." << std::endl;
                    }
                } catch (...) {
                    std::cout << "Exception on massive insert " << i << " - continuing..." << std::endl;
                }
            }
        }
    }

    // *** COMPREHENSIVE DATABASE DUMP AFTER INSERTIONS ***
    std::cout << "\n=== POST-INSERTION MABAIN DATABASE DUMP ===" << std::endl;

    try {
        std::cout << "Database status after insertions: " << db->StatusStr() << std::endl;
        std::cout << "Database is_open() after insertions: " << (db->is_open() ? "TRUE" : "FALSE") << std::endl;

        // Test database functionality after insertions
        std::cout << "\n--- Post-Insertion Database Functionality Test ---" << std::endl;

        // Try to lookup some of the inserted keys
        for (int test_i = 0; test_i < std::min(10, successful_insertions); test_i++) {
            std::stringstream test_key_ss;
            test_key_ss << "test_key_" << test_i;
            std::string test_key = test_key_ss.str();

            MBData lookup_mbd;
            int lookup_result = db->Find(test_key, lookup_mbd);
            std::cout << "Lookup " << test_key << ": result=" << lookup_result;

            if (lookup_result == 0) {
                std::string found_value(reinterpret_cast<const char*>(lookup_mbd.buff), lookup_mbd.data_len);
                std::cout << ", value=" << found_value.substr(0, 30) << "..." << std::endl;
            } else {
                std::cout << " (failed)" << std::endl;
            }
        }

        // Check database files after insertions
        std::cout << "\n--- Database Files After Insertions ---" << std::endl;
        const std::string db_dir = "/tmp/mabain_test_db/";
        std::vector<std::string> db_files = {
            db_dir + "_mabain_i", // Index file
            db_dir + "_mabain_d" // Data file
        };

        for (const auto& file : db_files) {
            struct stat file_stat;
            if (stat(file.c_str(), &file_stat) == 0) {
                std::cout << "Post-insertion File: " << file << std::endl;
                std::cout << "  Size: " << file_stat.st_size << " bytes" << std::endl;
                std::cout << "  Blocks: " << file_stat.st_blocks << " (512-byte blocks)" << std::endl;
                std::cout << "  Allocated: " << (file_stat.st_blocks * 512) << " bytes" << std::endl;

                if (static_cast<size_t>(file_stat.st_blocks * 512) < static_cast<size_t>(file_stat.st_size)) {
                    std::cout << "  ⚠️ STILL SPARSE after insertions!" << std::endl;
                    std::cout << "  Sparse gap: " << (static_cast<size_t>(file_stat.st_size) - static_cast<size_t>(file_stat.st_blocks * 512)) << " bytes" << std::endl;
                } else {
                    std::cout << "  ✓ Fully allocated after insertions" << std::endl;
                }
                std::cout << std::endl;
            }
        }

    } catch (const std::exception& e) {
        std::cout << "Exception during post-insertion database dump: " << e.what() << std::endl;
    } catch (...) {
        std::cout << "Unknown exception during post-insertion database dump" << std::endl;
    }

    std::cout << "=== END POST-INSERTION DATABASE DUMP ===" << std::endl;

    if (bus_error_occurred) {
        std::cout << "\n*** SIGBUS DETECTED DURING MABAIN INSERTIONS! ***" << std::endl;
        std::cout << "Total SIGBUS count: " << bus_error_count << std::endl;
        std::cout << "This occurred because:" << std::endl;
        std::cout << "1. Database files were created with ftruncate() (sparse allocation)" << std::endl;
        std::cout << "2. Disk was filled to 100% capacity after database creation" << std::endl;
        std::cout << "3. Insertions triggered file growth attempts" << std::endl;
        std::cout << "4. Memory-mapped access to sparse regions failed" << std::endl;
        std::cout << "5. Page fault failed due to no disk space = SIGBUS" << std::endl;
        std::cout << "\n*** THIS DEMONSTRATES THE ftruncate() PROBLEM! ***" << std::endl;
    } else {
        std::cout << "\nNo SIGBUS occurred during insertions" << std::endl;
        std::cout << "Possible reasons:" << std::endl;
        std::cout << "- Database files may have been allocated with fallocate() (GOOD!)" << std::endl;
        std::cout << "- File system may have reserved space for existing files" << std::endl;
        std::cout << "- Database operations stayed within pre-allocated regions" << std::endl;
        std::cout << "- However, the RISK still exists in production with ftruncate()!" << std::endl;
    }
}

int main()
{
    std::cout << "=== Mabain Database 100K Records Test (100MB Capacity) ===" << std::endl;
    std::cout << "This test validates that Mabain can handle 100K records with 100MB capacity:" << std::endl;
    std::cout << "1. Open Mabain database with 100MB capacity" << std::endl;
    std::cout << "2. Fill disk to 100% capacity in separate process" << std::endl;
    std::cout << "3. Insert 100K records (should succeed with proper allocation)" << std::endl;
    std::cout << "4. Monitor for SIGBUS issues with sparse files" << std::endl;

    // Phase 1: Open Mabain database first (while space exists)
    std::cout << "\nPhase 1: Opening Mabain database with 100MB capacity..." << std::endl;

    const std::string db_dir = "/tmp/mabain_test_db/";

    // Clean up any existing database
    int result = system(("rm -rf " + db_dir).c_str());
    (void)result; // Suppress unused warning
    result = system(("mkdir -p " + db_dir).c_str());
    (void)result; // Suppress unused warning

    // Install SIGBUS handler
    struct sigaction sa;
    sa.sa_sigaction = sigbus_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_SIGINFO;
    sigaction(SIGBUS, &sa, nullptr);

    // Configure Mabain with 100MB memory cap to handle 100K pairs comfortably
    size_t total_capacity = 100 * 1024 * 1024; // 100MB total capacity
    size_t index_capacity = total_capacity / 4; // 25MB index capacity
    size_t data_capacity = 3 * total_capacity / 4; // 75MB data capacity

    // Open database in writer mode
    DB* db = nullptr;
    try {
        db = new DB(db_dir.c_str(), CONSTS::WriterOptions(),
            index_capacity, data_capacity);

        if (!db->is_open()) {
            std::cerr << "Failed to open database: " << db->StatusStr() << std::endl;
            return 1;
        }

        // Set global pointer for SIGBUS handler access
        global_db_ptr = db;

        std::cout << "✓ Mabain database opened successfully" << std::endl;
        std::cout << "Database directory: " << db_dir << std::endl;
        std::cout << "Index capacity: " << index_capacity << " bytes (" << (index_capacity / 1024 / 1024) << " MB)" << std::endl;
        std::cout << "Data capacity: " << data_capacity << " bytes (" << (data_capacity / 1024 / 1024) << " MB)" << std::endl;
        std::cout << "Total capacity: " << (index_capacity + data_capacity) << " bytes (" << ((index_capacity + data_capacity) / 1024 / 1024) << " MB)" << std::endl;
        std::cout << "*** USING 100MB CAPACITY TO HANDLE 100K PAIRS COMFORTABLY ***" << std::endl;

        // Print initial database status
        std::cout << "\n--- Initial Database Status ---" << std::endl;
        std::cout << "Database status: " << db->StatusStr() << std::endl;
        std::cout << "Database is open: " << (db->is_open() ? "YES" : "NO") << std::endl;

        // Debug: Check which allocation method is actually being used
        std::cout << "\nDEBUG: Checking database file allocation method..." << std::endl;
        std::string index_file = db_dir + "_mabain_i";
        std::string data_file = db_dir + "_mabain_d";

        // Check if files exist and their sizes
        struct stat index_stat, data_stat;
        if (stat(index_file.c_str(), &index_stat) == 0) {
            std::cout << "DEBUG: Index file size: " << index_stat.st_size << " bytes" << std::endl;
            std::cout << "DEBUG: Index file blocks: " << index_stat.st_blocks << " (512-byte blocks)" << std::endl;
            size_t allocated_bytes = index_stat.st_blocks * 512;
            size_t file_size = static_cast<size_t>(index_stat.st_size);
            if (allocated_bytes < file_size) {
                std::cout << "DEBUG: ⚠️  Index file is SPARSE (allocated: " << allocated_bytes
                          << ", size: " << file_size << ")" << std::endl;
                std::cout << "DEBUG: ⚠️  This indicates ftruncate() is still being used!" << std::endl;
            } else {
                std::cout << "DEBUG: ✓ Index file is fully allocated (fallocate working)" << std::endl;
            }
        }

        if (stat(data_file.c_str(), &data_stat) == 0) {
            std::cout << "DEBUG: Data file size: " << data_stat.st_size << " bytes" << std::endl;
            std::cout << "DEBUG: Data file blocks: " << data_stat.st_blocks << " (512-byte blocks)" << std::endl;
            size_t allocated_bytes = data_stat.st_blocks * 512;
            size_t file_size = static_cast<size_t>(data_stat.st_size);
            if (allocated_bytes < file_size) {
                std::cout << "DEBUG: ⚠️  Data file is SPARSE (allocated: " << allocated_bytes
                          << ", size: " << file_size << ")" << std::endl;
                std::cout << "DEBUG: ⚠️  This indicates ftruncate() is still being used!" << std::endl;
            } else {
                std::cout << "DEBUG: ✓ Data file is fully allocated (fallocate working)" << std::endl;
            }
        }

        // Print comprehensive database state
        std::cout << "\n--- Comprehensive Database Information ---" << std::endl;

        // Try to get database statistics if available
        std::cout << "Attempting to print database statistics..." << std::endl;
        try {
            // Print basic database info
            std::cout << "Database directory: " << db_dir << std::endl;
            std::cout << "Database status string: " << db->StatusStr() << std::endl;

            // Try to perform a simple lookup to test database functionality
            std::string test_key = "initial_test_key";
            std::string test_value = "initial_test_value";
            std::cout << "Testing initial database operation..." << std::endl;

            // Test insert
            int insert_result = db->Add(test_key, test_value);
            std::cout << "Initial insert result: " << insert_result << std::endl;

            if (insert_result == 0) {
                // Test lookup
                MBData mbd;
                int lookup_result = db->Find(test_key, mbd);
                std::cout << "Initial lookup result: " << lookup_result << std::endl;
                if (lookup_result == 0) {
                    std::string retrieved_value(reinterpret_cast<const char*>(mbd.buff), mbd.data_len);
                    std::cout << "Retrieved value: " << retrieved_value << std::endl;
                    std::cout << "Retrieved data length: " << mbd.data_len << std::endl;
                    std::cout << "✓ Basic database operations working correctly" << std::endl;
                } else {
                    std::cout << "⚠️ Lookup failed with code: " << lookup_result << std::endl;
                }
            } else {
                std::cout << "⚠️ Initial insert failed with code: " << insert_result << std::endl;
            }

        } catch (const std::exception& e) {
            std::cout << "Exception during initial database test: " << e.what() << std::endl;
        } catch (...) {
            std::cout << "Unknown exception during initial database test" << std::endl;
        }

        // Print file system information for database files
        std::cout << "\n--- Database File System Analysis ---" << std::endl;
        std::vector<std::string> db_files = {
            db_dir + "_mabain_i", // Index file
            db_dir + "_mabain_d", // Data file
            db_dir + "_mabain_l" // Lock file (if exists)
        };

        for (const auto& file : db_files) {
            struct stat file_stat;
            if (stat(file.c_str(), &file_stat) == 0) {
                std::cout << "File: " << file << std::endl;
                std::cout << "  Size: " << file_stat.st_size << " bytes" << std::endl;
                std::cout << "  Blocks: " << file_stat.st_blocks << " (512-byte blocks)" << std::endl;
                std::cout << "  Allocated: " << (file_stat.st_blocks * 512) << " bytes" << std::endl;
                std::cout << "  Mode: " << std::oct << file_stat.st_mode << std::dec << std::endl;
                std::cout << "  Links: " << file_stat.st_nlink << std::endl;

                if (static_cast<size_t>(file_stat.st_blocks * 512) < static_cast<size_t>(file_stat.st_size)) {
                    std::cout << "  ⚠️ SPARSE FILE DETECTED!" << std::endl;
                } else {
                    std::cout << "  ✓ Fully allocated file" << std::endl;
                }
                std::cout << std::endl;
            } else {
                std::cout << "File: " << file << " (not found)" << std::endl;
            }
        }
    } catch (...) {
        std::cerr << "Exception opening database" << std::endl;
        global_db_ptr = nullptr;
        return 1;
    }

    // Phase 2: Now fill disk to 100% in separate process
    std::cout << "\nPhase 2: Filling disk to 100% capacity..." << std::endl;
    pid_t pressure_pid = create_disk_pressure_process();
    if (pressure_pid <= 0) {
        std::cout << "Failed to create disk pressure process. Test results may not be conclusive." << std::endl;
        global_db_ptr = nullptr;
        delete db;
        return 1;
    }

    // Phase 3: Try Mabain operations (should handle 100K records with 100MB capacity)
    std::cout << "\nPhase 3: Testing 100K insertions with 100MB capacity..." << std::endl;
    test_mabain_insertions_only(db, index_capacity, data_capacity);

    // Clean up
    std::cout << "\nPhase 4: Cleaning up..." << std::endl;

    // Clear global pointer before deleting
    global_db_ptr = nullptr;
    delete db;
    cleanup_disk_pressure_process(pressure_pid);

    // Remove database directory
    result = system(("rm -rf " + db_dir).c_str());
    (void)result; // Suppress unused warning

    std::cout << "\n=== Summary ===" << std::endl;
    if (bus_error_occurred) {
        std::cout << "✓ SUCCESSFULLY REPRODUCED the Mabain SIGBUS issue!" << std::endl;
        std::cout << "✓ ftruncate() creates sparse files that cause SIGBUS under disk pressure" << std::endl;
        std::cout << "✓ Mabain database operations to sparse regions trigger page faults" << std::endl;
        std::cout << "✓ Page faults fail when no disk space is available = SIGBUS" << std::endl;
        std::cout << "✓ Total Mabain SIGBUS count: " << bus_error_count << std::endl;
        std::cout << "\n*** PROOF: ftruncate() is dangerous for memory-mapped database files! ***" << std::endl;
    } else {
        std::cout << "• No SIGBUS occurred during Mabain operations in this test run" << std::endl;
        std::cout << "• This suggests fallocate() is working correctly (GOOD!)" << std::endl;
        std::cout << "• OR filesystem-specific behavior prevented SIGBUS" << std::endl;
        std::cout << "• The RISK still exists with ftruncate() in production!" << std::endl;
    }

    std::cout << "\n🔧 SOLUTION: Use fallocate() instead of ftruncate()" << std::endl;
    std::cout << "• fallocate() ensures real disk space allocation upfront" << std::endl;
    std::cout << "• Prevents unexpected SIGBUS during Mabain database operations" << std::endl;
    std::cout << "• Provides predictable behavior for memory-mapped database files" << std::endl;
    std::cout << "• Database operations either succeed completely or fail gracefully" << std::endl;
    std::cout << "• IMPLEMENTATION: Change TruncateFile() to AllocateFile() in mmap_file.cpp" << std::endl;

    return bus_error_occurred ? 1 : 0;
}

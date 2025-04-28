#include "FileWatcher.h"
#include "../logger/Logger.h"

#include <sys/inotify.h>
#include <unistd.h>
#include <thread>
#include <filesystem>
#include <cstring>

namespace fs = std::filesystem;

void FileWatcher::watchFile(const std::string& path, const std::function<void()>& onChangeCallback, const std::atomic<bool>& stopFlag) {
    std::thread([path, onChangeCallback, &stopFlag]() {
        int inotifyFd = inotify_init1(IN_NONBLOCK);
        if (inotifyFd < 0) {
            Logger::error("❌ FileWatcher failed to init inotify.");
            return;
        }

        std::string directory = fs::path(path).parent_path();
        std::string filename = fs::path(path).filename();

        int wdFile = inotify_add_watch(inotifyFd, path.c_str(), IN_MODIFY | IN_DELETE_SELF | IN_MOVE_SELF);
        int wdDir = inotify_add_watch(inotifyFd, directory.c_str(), IN_CREATE | IN_MOVED_TO);

        if (wdFile < 0 || wdDir < 0) {
            Logger::warn("⚠️ FileWatcher failed to add watch for file or directory: " + path);
        } else {
            Logger::info("👀 FileWatcher is watching: " + path);
        }

        constexpr size_t BUF_LEN = 4096;
        char buf[BUF_LEN] __attribute__ ((aligned(__alignof__(struct inotify_event))));

        while (!stopFlag) {
            int numRead = read(inotifyFd, buf, BUF_LEN);
            if (numRead <= 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            for (char* ptr = buf; ptr < buf + numRead; ) {
                struct inotify_event* event = (struct inotify_event*)ptr;

                if (event->wd == wdFile) {
                    if (event->mask & IN_MODIFY) {
                        Logger::info("🔁 FileWatcher detected modify: " + path);
                        onChangeCallback();
                    }
                    if (event->mask & (IN_DELETE_SELF | IN_MOVE_SELF)) {
                        Logger::warn("⚠️ FileWatcher: File deleted or moved. Re-watching: " + path);
                        inotify_rm_watch(inotifyFd, wdFile);
                        wdFile = -1;
                    }
                }

                if (event->wd == wdDir && event->len > 0) {
                    std::string createdFile(event->name);
                    if (createdFile == filename) {
                        Logger::info("🆕 FileWatcher: Detected recreate of file: " + path);
                        if (wdFile == -1) {
                            wdFile = inotify_add_watch(inotifyFd, path.c_str(), IN_MODIFY | IN_DELETE_SELF | IN_MOVE_SELF);
                            if (wdFile >= 0) {
                                Logger::info("✅ FileWatcher re-watching file after recreate: " + path);
                                onChangeCallback();
                            }
                        }
                    }
                }

                ptr += sizeof(struct inotify_event) + event->len;
            }
        }

        if (wdFile >= 0) inotify_rm_watch(inotifyFd, wdFile);
        if (wdDir >= 0) inotify_rm_watch(inotifyFd, wdDir);
        close(inotifyFd);
    }).detach();
}


#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using dfs_service::DFSService;
using dfs_service::FileData;
using dfs_service::FileInfo;
using dfs_service::FileList;
using dfs_service::RequestFile;
using dfs_service::ReturnFileInfo;
using dfs_service::ReturnMsg;
using dfs_service::Void;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = RequestFile;
using FileListResponseType = FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

std::mutex dir_mutex;

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    RequestFile request_file;
    Void void_;

    request_file.set_name(filename);
    request_file.set_request_client_id(ClientId());

    Status status_code = service_stub->RequestWriteLock(&context, request_file, &void_);
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Client successfully received write lock from server: " << filename;
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to receive write lock from server: " << filename;  
    }
    return status_code.error_code();
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;

    std::string file_path = WrapPath(filename);
    FileInfo file_info; 
    FileData file_data;

    /* Check if file exists */
    struct stat st;
    if (stat(file_path.c_str(), &st) != 0) {
        dfs_log(LL_ERROR) << "File not found or fail to open: " << file_path;
        return StatusCode::NOT_FOUND;
    }

    /* Acquire write lock */
    if (this->RequestWriteAccess(filename) != StatusCode::OK) {
        dfs_log(LL_ERROR) << "Fail to acquire a write lock";
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    std::unique_ptr <ClientWriter<FileData>> client_writer = service_stub->StoreFile(&context, &file_info);      
    dfs_log(LL_SYSINFO) << "Client starts storing file to server: " << file_path;
    
    /* Sending information related to file and client */
    size_t file_size = st.st_size;
    long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
    long client_crc = dfs_file_checksum(file_path, &crc_table);
    size_t bytes_sent = 0, total_sent = 0;
    char buffer[BUFSIZE]; 

    file_data.set_data(filename);
    client_writer->Write(file_data);
    file_data.set_data(ClientId());
    client_writer->Write(file_data);
    file_data.set_data(std::to_string(mdf_time));
    client_writer->Write(file_data);
    file_data.set_data(std::to_string(client_crc));
    client_writer->Write(file_data);

    std::ifstream ifs;
    ifs.open(file_path);
    if (!ifs) {
        dfs_log(LL_ERROR) << "File not found or fail to open: " << file_path;
        return StatusCode::NOT_FOUND;
    }

    while (total_sent < file_size) {
        if ((file_size - total_sent) >= (BUFSIZE - 1)) {
            bytes_sent = BUFSIZE - 1;
        }
        else {
            bytes_sent = file_size - total_sent;
        }        

        if (ifs.good() && ifs.is_open()) {
            ifs.read(buffer, bytes_sent);
            file_data.set_data(buffer, bytes_sent);
            client_writer->Write(file_data);
            total_sent += bytes_sent;
        }
        memset(buffer, 0, BUFSIZE);
    }
    ifs.close();

    if (total_sent != file_size) {
        dfs_log(LL_ERROR) << "Client failed to send complete data";
        return StatusCode::CANCELLED;
    }

    /* 4. Check status */
    client_writer->WritesDone();
    Status status_code = client_writer->Finish();
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Client successfully send file: " << filename << " to server";
        return status_code.error_code();
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to send file: " << filename;
        return status_code.error_code();
    }
}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    ClientContext context;

    std::string file_path = WrapPath(filename);
    RequestFile request_file;
    FileData file_data;

    /* Check if file exists */
    struct stat st;
    if (stat(file_path.c_str(), &st) == 0) {
        dfs_log(LL_ERROR) << "File found locally: " << file_path;
        long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
        request_file.set_request_mdf_time(mdf_time);
    }

    request_file.set_name(filename);
    long crc = dfs_file_checksum(file_path, &crc_table);
    request_file.set_client_file_crc(crc);
    std::unique_ptr <ClientReader<FileData>> client_reader = service_stub->FetchFile(&context, request_file);   
    std::ofstream ofs;

    /* 2. Receive data from server */
    while (client_reader->Read(&file_data)) {
        // This is not surs yet
        if (!ofs.is_open()) {
            ofs.open(file_path, std::ios::trunc);
        }

        const std::string &data = file_data.data();
        ofs << data;
    }
    ofs.close();

    Status status_code = client_reader->Finish();
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Client successfully received file from server: " << filename;
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to receive file from server: " << filename;
    } 
    return status_code.error_code();
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    std::string file_path = WrapPath(filename);
    FileInfo file_info; 
    RequestFile request_file;

    /* Acquire write lock */
    if (this->RequestWriteAccess(filename) != StatusCode::OK) {
        dfs_log(LL_ERROR) << "Fail to acquire a write lock";
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    // TODO: do we need to set time limit?

    request_file.set_name(filename);
    request_file.set_request_client_id(ClientId());

    /* Send delete file request */
    Status status_code = service_stub->DeleteFile(&context, request_file, &file_info); 
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Client successfully deleted file from server: " << filename;
        return status_code.error_code();
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to delete file from server: " << filename;
        return status_code.error_code();        
    }
}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);   

    Void request;
    FileList file_list;
    FileInfo file_info;

    Status status_code = service_stub->ListFiles(&context, request, &file_list);
    if (status_code.ok()) {
        for (int i = 0; i < file_list.files_size(); i++) {
            const FileInfo &file_info = file_list.files(i);
            file_map->insert(std::pair<std::string, int>(file_info.name(), file_info.mdf_time()));
            dfs_log(LL_SYSINFO) << "File name: " << file_info.name() 
                                << " Last Modified time: " << file_info.mdf_time();
        }
        return status_code.error_code();
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to retrieve information from server";
        return status_code.error_code();
    }
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(DFS_RESET_TIMEOUT);
    context.set_deadline(deadline);   

    /* 1. Initialization */
    FileInfo file_info;
    RequestFile request_file;
    request_file.set_name(filename);    

    /* 2. Receive file status */
    Status status_code = service_stub->GetFileStatus(&context, request_file, &file_info);
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Status of file: " << filename;
        dfs_log(LL_SYSINFO) << "File size: " << file_info.file_size();
        dfs_log(LL_SYSINFO) << "Last modified time: " << file_info.mdf_time();
        dfs_log(LL_SYSINFO) << "Created time: " << file_info.crt_time();
        return status_code.error_code();  
    }
    else {
        dfs_log(LL_ERROR) << "Client receive file status for: " << filename;
        return status_code.error_code();  
    }
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    std::lock_guard<std::mutex> lock(dir_mutex);
    callback();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                std::lock_guard<std::mutex> lock(dir_mutex);
                
                for (const FileInfo &file_info_from_server : call_data->reply.files()) {
                    FileInfo file_info_from_client;
                    std::string file_name = file_info_from_server.name();
                    std::string file_path = WrapPath(file_name);
                    
                    long mdf_time_from_server = file_info_from_server.mdf_time();
                    long mdf_time_from_client = file_info_from_client.mdf_time();

                    struct stat st;
                    if (stat(file_path.c_str(), &st) != 0) {
                        this->Fetch(file_name);
                    }
                    else if (mdf_time_from_server == mdf_time_from_client) {

                    }
                    else if (mdf_time_from_server < mdf_time_from_client) {
                        this->Store(file_name);
                    }
                    else if (mdf_time_from_server > mdf_time_from_client) {
                        StatusCode status_code = this->Fetch(file_name);
                        if (status_code == StatusCode::ALREADY_EXISTS) {
                            struct utimbuf new_times;
                            new_times.actime = st.st_atime;
                            new_times.modtime = mdf_time_from_server;   
                            utime(file_path.c_str(), &new_times);
                        }
                    }
                }

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//
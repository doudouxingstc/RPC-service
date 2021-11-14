#include <regex>
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
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
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
using dfs_service::ReturnMsg;
using dfs_service::Void;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);

    /* 1. Initialization */
    std::string file_path = WrapPath(filename);
    FileInfo file_info; 
    
    std::unique_ptr <ClientWriter<FileData>> client_writer = service_stub->StoreFile(&context, &file_info);  // ???

    std::ifstream ifs;
    ifs.open(file_path);
    if (!ifs) {
        dfs_log(LL_ERROR) << "File not found or fail to open: " << file_path;
        return StatusCode::NOT_FOUND;
    }
    
    FileData file_data;
    size_t file_size;
    size_t bytes_sent = 0, total_sent = 0;
    char buffer[BUFSIZE];

    struct stat st;
    stat(file_path.c_str(), &st);
    file_size = st.st_size;

    /* 2. Sending file name */
    file_data.set_data(filename);
    client_writer->Write(file_data);

    /* 3. Sending file data */
    dfs_log(LL_SYSINFO) << "Client start to store file to server: " << file_path;

    while(total_sent < file_size) {
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
        memset(buffer, 0, sizeof(char)*BUFSIZE);
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


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
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
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);

    /* 1. Initialization */
    RequestFile request_file;
    request_file.set_request_file_name(filename);
    std::string file_path = WrapPath(filename);
    std::unique_ptr <ClientReader<FileData>> client_reader = service_stub->FetchFile(&context, request_file);   
    std::ofstream ofs;

    /* 2. Receive data from server */
    dfs_log(LL_SYSINFO) << "Client starts to receive file from server: " << file_path;
    FileData file_data;

    while (client_reader->Read(&file_data)) {
        // This is not sure yet
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
        return status_code.error_code();
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to receive file from server: " << filename;
        return status_code.error_code();
    }
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {
    
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
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
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);   

    /* 1. Initialization */
    ReturnMsg return_msg;
    RequestFile request_file;
    request_file.set_request_file_name(filename);

    /* 2. Send delete file request */
    Status status_code = service_stub->DeleteFile(&context, request_file, &return_msg);
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Client successfully deleted file from server: " << filename;
        return status_code.error_code();
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to delete file from server: " << filename;
        return status_code.error_code();        
    }
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
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

    Status status_code = service_stub->ListFile(&context, request, &file_list);
    if (status_code.ok()) {
        for (int i = 0; i < file_list.files_size(); i++) {
            const FileInfo &file_info = file_list.files(i);
            std::string file_name = file_info.file_name();
            long mdf_time = file_info.mdf_time();
            file_map->insert(std::pair<std::string, long>(file_name, mdf_time));
            dfs_log(LL_SYSINFO) << "File name: " << file_info.file_name() 
                                << " Last Modified time: " << file_info.mdf_time();
        }
    }
    else {
        dfs_log(LL_ERROR) << "Client failed to retrieve information from server";
    }
    return status_code.error_code();
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
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
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout);
    context.set_deadline(deadline);   

    /* 1. Initialization */
    FileInfo file_info;
    RequestFile request_file;
    request_file.set_request_file_name(filename);    

    /* 2. Receive file status */
    Status status_code = service_stub->GetFileStatus(&context, request_file, &file_info);
    if (status_code.ok()) {
        dfs_log(LL_SYSINFO) << "Status of file: " << filename;
        dfs_log(LL_SYSINFO) << "File size: " << file_info.file_size();
        dfs_log(LL_SYSINFO) << "Last modified time: " << file_info.mdf_time();
        dfs_log(LL_SYSINFO) << "Created time: " << file_info.crt_time();
    }
    else {
        dfs_log(LL_ERROR) << "Client receive file status for: " << filename; 
    }
    return status_code.error_code();      
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//
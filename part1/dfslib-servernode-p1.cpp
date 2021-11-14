#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

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
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     * 
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    Status StoreFile(ServerContext *context, 
            ServerReader<FileData> *server_reader, FileInfo *file_info) override {
        /* 1. Read file name */
        FileData file_data;
        std::string file_name;
        std::string file_path;
        std::ofstream ofs;

        server_reader->Read(&file_data);
        file_name = file_data.data();
        file_path = WrapPath(file_name);
        
        /* 2. Receive file data */
        dfs_log(LL_SYSINFO) << "Server starts storing data to file: " << file_name;

        while (server_reader->Read(&file_data)) {
            // Check for deadline
            if (context->IsCancelled()) {
                std::string error_msg = "Deadline exceeded or Client cancelled, abandoning";
                dfs_log(LL_ERROR) << error_msg;
                return Status(StatusCode::DEADLINE_EXCEEDED, error_msg);
            }

            if (!ofs.is_open()) {
                ofs.open(file_path, std::ios::trunc);
            }
            std::string data = file_data.data();
            ofs << data;
        }
        ofs.close();

        /* 3. Update the information for FileInfo variable */
        struct stat st;
        stat(file_path.c_str(), &st);
        dfs_log(LL_SYSINFO) << "Server successfully stored data of size " << st.st_size;

        long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
        long crt_time = static_cast<long> (st.st_ctim.tv_sec);
        file_info->set_mdf_time(mdf_time); 
        file_info->set_crt_time(crt_time); 
        file_info->set_file_name(file_name);
        file_info->set_file_size(st.st_size);
        // file_info->set_mdf_time(st.st_mtim.tv_sec); 
        // file_info->set_crt_time(st.st_ctim.tv_sec); 
        return Status::OK;
    }


    Status FetchFile(ServerContext *context,
            const RequestFile *request_file, ServerWriter<FileData> *server_writer) override {
        /* 1. Make path */
        std::string file_name = request_file->request_file_name();
        std::string file_path = WrapPath(file_name);

        /* 2. Check if the file is in server */
        FileData file_data;
        char buffer[BUFSIZE];
        std::ifstream ifs;

        ifs.open(file_path, std::ios::in);
        if (!ifs) {
            std::stringstream str_stream;
            str_stream << "File not found for " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();
            return Status(StatusCode::NOT_FOUND, str_stream.str());
        }

        /* 3. Send file data in chunks to client */
        size_t bytes_sent = 0, total_sent = 0;
        struct stat st;
        if (stat(file_path.c_str(), &st) == -1) {
            dfs_log(LL_ERROR) << "Unknown error at server";
            return Status(StatusCode::INTERNAL, "Unknown error at server");
        }        
        size_t file_size = st.st_size;
        
        dfs_log(LL_SYSINFO) << "Server starts sending data for file: " << file_name;
        
        while (total_sent < file_size) {
            // Check for deadline
            if (context->IsCancelled()) {
                const std::string &error_msg = "Deadline exceeded or Client cancelled, abandoning";
                dfs_log(LL_ERROR) << error_msg;
                return Status(StatusCode::DEADLINE_EXCEEDED, error_msg);
            }

            if ((file_size - total_sent) >= (BUFSIZE - 1)) {
                bytes_sent = BUFSIZE - 1;
            }
            else {
                bytes_sent = file_size - total_sent;
            }

            if (ifs.good() && ifs.is_open()) {
                ifs.read(buffer, bytes_sent);
                file_data.set_data(buffer, bytes_sent);
                server_writer->Write(file_data);
                total_sent += bytes_sent;
            }
        }
        ifs.close();

        if (total_sent != file_size) {
            dfs_log(LL_ERROR) << "Server failed to send complete data";
            return Status(StatusCode::INTERNAL, "Server failed to send complete data");
        }

        dfs_log(LL_SYSINFO) << "Server successful send file: " << file_name;
        return Status::OK;
    }


    Status DeleteFile(ServerContext *context,
            const RequestFile *request_file, ReturnMsg *return_msg) override {
        /* 1. Make path */
        std::string file_name = request_file->request_file_name();
        std::string file_path = WrapPath(file_name);        

        /* 2. Check if the file is in the server */
        struct stat st;
        if (stat(file_path.c_str(), &st) == -1) {
            std::stringstream str_stream;
            str_stream << "File not found for " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();
            return Status(StatusCode::NOT_FOUND, str_stream.str());
        }        

        /* 3. Delete the file requested */
        int rv = remove(file_path.c_str());
        if (rv == -1) {
            std::stringstream str_stream;
            str_stream << "Server fail to delete " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();
            return Status(StatusCode::INTERNAL, str_stream.str());            
        }

        dfs_log(LL_SYSINFO) << "Server sucessfully deleted the file " << file_name;
        return_msg->set_msg("Server sucessfully deleted the file");
        return Status::OK;
    }
    

    Status ListFile(ServerContext *context,
            const Void *void_, FileList *file_list) override {
        /* Make the directory struct to iterate objects in the directory */
        DIR *dir;
        struct dirent *ent;

        if ((dir = opendir(mount_path.c_str())) != NULL) {
            while ((ent = readdir(dir)) != NULL) {
                if (context->IsCancelled()) {
                    std::string error_msg = "Deadline exceeded or Client cancelled, abandoning";
                    dfs_log(LL_ERROR) << error_msg;
                    return Status(StatusCode::DEADLINE_EXCEEDED, error_msg);
                }                

                std::string file_name(ent->d_name);
                std::string file_path = WrapPath(file_name);

                FileInfo *file_info = file_list->add_files(); 
                struct stat st;
                if ((stat(file_path.c_str(), &st)) == -1) {
                    std::stringstream str_stream;
                    str_stream << "Server fail to open up: " << file_path;
                    dfs_log(LL_ERROR) << str_stream.str();
                    file_info->set_file_name(file_name);
                    continue;                    
                }

                long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
                long crt_time = static_cast<long> (st.st_ctim.tv_sec);
                file_info->set_mdf_time(mdf_time); 
                file_info->set_crt_time(crt_time); 
                file_info->set_file_name(file_name);
                file_info->set_file_size(st.st_size);
                // file_info->set_mdf_time(st.st_mtim.tv_sec); 
                // file_info->set_crt_time(st.st_ctim.tv_sec); 
                dfs_log(LL_SYSINFO) << "Found file: " << file_path;
            }
        }
        else {
            std::string error_msg = "Server failed to open directory";
            dfs_log(LL_ERROR) << error_msg;
            return Status(StatusCode::INTERNAL, error_msg);            
        }
        closedir(dir);

        dfs_log(LL_SYSINFO) << "All files have been listed";
        return Status::OK;
    }


    Status GetFileStatus(ServerContext *context,
            const RequestFile *request_file, FileInfo *file_info) override {
         /* 1. Make path */
        std::string file_name = request_file->request_file_name();
        std::string file_path = WrapPath(file_name); 

        /* 2. Check if the file is in the server */           
        struct stat st;
        if (stat(file_path.c_str(), &st) == -1) {
            std::stringstream str_stream;
            str_stream << "File not found for " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();
            return Status(StatusCode::NOT_FOUND, str_stream.str());
        }              

        /* 3. Return the status of the file */
        long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
        long crt_time = static_cast<long> (st.st_ctim.tv_sec);
        file_info->set_mdf_time(mdf_time); 
        file_info->set_crt_time(crt_time); 
        file_info->set_file_name(file_name);
        file_info->set_file_size(st.st_size);
        // file_info->set_file_name(file_name);
        // file_info->set_file_size(st.st_size);
        // file_info->set_mdf_time(st.st_mtim.tv_sec); 
        // file_info->set_crt_time(st.st_ctim.tv_sec);          
        return Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//
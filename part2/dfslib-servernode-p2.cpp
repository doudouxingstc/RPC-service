#include <map>
#include <mutex>
#include <shared_mutex>
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
#include <sys/time.h>
#include <grpcpp/grpcpp.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

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
using dfs_service::ReturnFileInfo;
using dfs_service::ReturnMsg;
using dfs_service::Void;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = RequestFile;
using FileListResponseType = FileList;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    /* Server Directory Mutex */
    std::mutex dir_mutex;

    /* Map from file to current-owned client */
    std::map<std::string, std::string> file_client_map;

    /* Mutex of controlling the file-client map */
    std::mutex file_client_map_mutex;

    /* Map from file to its mutex */
    std::map<std::string, std::unique_ptr<std::mutex>> file_mutex_map;

    /* Mutex of controlling the file-mutex map */
    std::mutex file_mutex_map_mutex;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

        /* Traverse the entire directory, make the map for all files and their file-specific mutex */
        DIR *dir;
        struct dirent *ent;

        if ((dir = opendir(mount_path.c_str())) != NULL) {
            while ((ent = readdir(dir)) != NULL) {
                if (ent->d_type != DT_REG) {
                    continue;
                }

                std::string file_name(ent->d_name);
                file_mutex_map[file_name] = std::make_unique<std::mutex>();
                std::string file_path = WrapPath(file_name);
                dfs_log(LL_SYSINFO) << "Found File: " << file_path;
            }
            closedir(dir);
        }
        else {
            std::string error_msg = "Server failed to open directory";
            dfs_log(LL_ERROR) << error_msg;           
        }
    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        Status status_code = this->CallbackList(context, request, response);
        if (status_code.ok()) {
            dfs_log(LL_SYSINFO) << "Server handling listing files" << request->name();
        }
        else {
            dfs_log(LL_ERROR) << "Error at ProcessCallback: " << status_code.error_message();
        }

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //
    
    Status StoreFile(ServerContext *context, 
            ServerReader<FileData> *server_reader, FileInfo *return_file_info) override {
        FileData file_data;

        /* 1. Receive file information: file nama, file mtime, client id, etc. */
        std::string file_name;
        std::string client_id;
        long mdf_time;
        long client_crc;
        try {
            server_reader->Read(&file_data);
            file_name = file_data.data();
            server_reader->Read(&file_data);
            client_id = file_data.data();
            server_reader->Read(&file_data);
            mdf_time = stol(file_data.data());
            server_reader->Read(&file_data);
            client_crc = stol(file_data.data());
        } catch (std::exception const &e){
            std::stringstream str_str;
            str_str << "Improper file info for file name/client ID/modified time/clietn CRC: " << e.what() << std::endl;
            dfs_log(LL_ERROR) << str_str.str();
            return Status(StatusCode::INTERNAL, str_str.str());
        }

        /* 2. Check if the file has a client owned */
        std::string file_path = WrapPath(file_name);

        file_client_map_mutex.lock();
        auto file_client_iter = file_client_map.find(file_name);
        if ((file_client_iter == file_client_map.end()) || ((file_client_iter->second).compare(client_id) != 0)) {
            std::stringstream str_str;
            str_str << client_id << " has no write lock for " << file_name << ", or the file has already been locked" << std::endl;
            dfs_log(LL_SYSINFO) << str_str.str();
            file_client_map_mutex.unlock();
            return Status(StatusCode::INTERNAL, str_str.str());
        }
        file_client_map_mutex.unlock();

        // Get mutex for the file
        file_mutex_map_mutex.lock();
        auto file_mutex_iter = file_mutex_map.find(file_name);
        std::mutex *file_mutex = file_mutex_iter->second.get();
        file_mutex_map_mutex.unlock();

        /* 3. Perform CRC check */
        std::lock_guard<std::mutex> lock(dir_mutex);
        std::lock_guard<std::mutex> lock2(*(file_mutex)); 

        long server_crc = dfs_file_checksum(file_path, &this->crc_table);
        if (server_crc == client_crc) {
            std::string msg1 = "File already exists";
            dfs_log(LL_SYSINFO) << msg1 << " for: " << file_name;

            struct stat st;
            stat(file_path.c_str(), &st);
            long s_mdf_time = static_cast<long> (st.st_mtim.tv_sec);

            if (s_mdf_time < mdf_time) {
                std::string msg = "Client modified time greater than server, now updating";
                dfs_log(LL_SYSINFO) << msg;

                struct utimbuf new_times;
                new_times.actime = st.st_atime;
                new_times.modtime = mdf_time;   
                utime(file_path.c_str(), &new_times);
            }

            file_mutex_map_mutex.lock();
            file_client_map.erase(file_name); 
            file_mutex_map_mutex.unlock();
            return Status(StatusCode::ALREADY_EXISTS, msg1);
        }

        /* 4. Store file data in server */
        std::ofstream ofs;
        dfs_log(LL_SYSINFO) << "Server starts storing data to file: " << file_name;
        
        while (server_reader->Read(&file_data)) {
            if (context->IsCancelled()) {
                std::string error_msg = "Deadline exceeded or Client cancelled, abandoning";
                dfs_log(LL_ERROR) << error_msg;
                std::lock_guard<std::mutex> lock(file_client_map_mutex);
                file_client_map.erase(file_name); 
                return Status(StatusCode::DEADLINE_EXCEEDED, error_msg);
            }

            if (!ofs.is_open()) {
                ofs.open(file_path, std::ios::trunc);
            }

            std::string data = file_data.data();
            ofs << data;            
        }
        struct stat st;
        stat(file_path.c_str(), &st);
        dfs_log(LL_SYSINFO) << "Server successfully stored data of size " << st.st_size;

        long return_mdf_time = static_cast<long> (st.st_mtim.tv_sec);
        long return_crt_time = static_cast<long> (st.st_ctim.tv_sec);
        return_file_info->set_mdf_time(return_mdf_time); 
        return_file_info->set_crt_time(return_crt_time); 
        return_file_info->set_name(file_name);
        return_file_info->set_file_size(st.st_size);
        
        /* 5. Remove allocated write lock */
        std::lock_guard<std::mutex> lock3(file_client_map_mutex);
        file_client_map.erase(file_name);       
        return Status::OK;
    }
    

    Status FetchFile(ServerContext *context,
            const RequestFile *request_file, ServerWriter<FileData> *server_writer) override {
        std::string file_name = request_file->name();
        std::string client_id = request_file->request_client_id();
        long mdf_time = request_file->request_mdf_time();
        long client_crc = request_file->client_file_crc();
        std::string file_path = WrapPath(file_name);
        
        /* 1. Ensure the file has corresponding write lock */
        file_mutex_map_mutex.lock();
        if (file_mutex_map.find(file_name) == file_mutex_map.end()) {
            file_mutex_map[file_name] = std::make_unique<std::mutex>();
        }
        auto file_mutex_iter = file_mutex_map.find(file_name);
        std::mutex *file_mutex = file_mutex_iter->second.get();
        file_mutex_map_mutex.unlock();

        
        std::lock_guard<std::mutex> lock(*file_mutex);

        /* 2. Check if the file is in server */
        struct stat st;
        if (stat(file_path.c_str(), &st) != 0) {
            std::stringstream str_stream;
            str_stream << "File not found for " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();
            return Status(StatusCode::NOT_FOUND, str_stream.str());
        }
        
        /* 3. Perform CRC checks */
        int server_crc = dfs_file_checksum(file_name, &this->crc_table);
        if (server_crc == client_crc) {
            std::string msg = "File already exists in local environment";
            dfs_log(LL_SYSINFO) << msg << " for: " << file_name;

            struct stat st;
            stat(file_path.c_str(), &st);
            long s_mdf_time = static_cast<long> (st.st_mtim.tv_sec);

            if (s_mdf_time < mdf_time) {
                std::string msg1 = "Client modified time greater than server, now updating";
                dfs_log(LL_SYSINFO) << msg1;

                struct utimbuf new_times;
                new_times.actime = st.st_atime;
                new_times.modtime = mdf_time;   
                utime(file_path.c_str(), &new_times);
            }

            file_mutex_map_mutex.lock();
            file_client_map.erase(file_name); 
            file_mutex_map_mutex.unlock();
            return Status(StatusCode::ALREADY_EXISTS, msg);
        }

        /* 4. Send file data */
        std::ifstream ifs;
        FileData file_data;
        char buffer[BUFSIZE];
        size_t bytes_sent = 0, total_sent = 0;
        size_t file_size = st.st_size;
        
        dfs_log(LL_SYSINFO) << "Server starts sending data for file: " << file_name;
        
        while (total_sent < file_size) {
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


    Status ListFiles(ServerContext *context, 
            const Void *void_, FileList *file_list) override {
        std::lock_guard<std::mutex> lock(dir_mutex);
        DIR *dir;
        struct dirent *ent;

        if ((dir = opendir(this->mount_path.c_str())) != NULL) {
            while ((ent = readdir(dir)) != NULL) {
                std::string file_name(ent->d_name);
                std::string file_path = WrapPath(file_name);

                FileInfo *file_info = file_list->add_files();
                struct stat st;
                if ((stat(file_path.c_str(), &st)) == -1) {
                    std::stringstream str_stream;
                    str_stream << "Server fail to open up: " << file_path;
                    dfs_log(LL_ERROR) << str_stream.str();
                    file_info->set_name(file_name);
                    continue;                    
                }

                long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
                long crt_time = static_cast<long> (st.st_ctim.tv_sec);
                file_info->set_mdf_time(mdf_time); 
                file_info->set_crt_time(crt_time);
                file_info->set_name(file_name);
                file_info->set_file_size(st.st_size);
                dfs_log(LL_SYSINFO) << "Found file " << file_path;
            }
        }
        else {
            std::string msg = "Server failed to open directory";
            dfs_log(LL_ERROR) << msg;
            return Status(StatusCode::INTERNAL, msg);
        }

        closedir(dir);
        return Status::OK;
    }


    Status GetFileStatus(ServerContext *context, 
            const RequestFile *request_file, FileInfo *file_info) override {
        std::string file_name = request_file->name();
        std::string client_id = request_file->request_client_id();
        //int client_crc = request_file->client_file_crc();
        std::string file_path = WrapPath(file_name);

        /* Ensure the file has corresponding lock */
        file_mutex_map_mutex.lock();
        if (file_mutex_map.find(file_name) == file_mutex_map.end()) {
            file_mutex_map[file_name] = std::make_unique<std::mutex>();
        }
        auto file_mutex_iter = file_mutex_map.find(file_name);
        std::mutex *file_mutex = file_mutex_iter->second.get();
        file_mutex_map_mutex.unlock();

        std::lock_guard<std::mutex> lock(*file_mutex);   
        struct stat st;
        if (stat(file_path.c_str(), &st) == -1) {
            std::stringstream str_stream;
            str_stream << "File not found for " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();
            return Status(StatusCode::ALREADY_EXISTS, str_stream.str());
        }    

        long mdf_time = static_cast<long> (st.st_mtim.tv_sec);
        long crt_time = static_cast<long> (st.st_ctim.tv_sec);
        file_info->set_mdf_time(mdf_time); 
        file_info->set_crt_time(crt_time);
        file_info->set_name(file_name);
        file_info->set_file_size(st.st_size);            
        return Status::OK;
    }


    Status RequestWriteLock(ServerContext *context, 
            const RequestFile *request_file, Void *void_) override {
        std::string file_name = request_file->name();
        std::string client_id = request_file->request_client_id();
        
        std::lock_guard<std::mutex> lock(file_client_map_mutex);
        auto client_id_lock = file_client_map.find(file_name);
        if (client_id_lock == file_client_map.end()) { 
            file_client_map[file_name] = client_id;
        }
        else {
            std::stringstream str_str;
            str_str << "Fail to acquire the lock for client ID: " << client_id;
            dfs_log(LL_ERROR) << str_str.str();
            return Status(StatusCode::INTERNAL, str_str.str());
        }

        /* 3. Create file-specific mutex for the client */
        std::lock_guard<std::mutex> lock1(file_mutex_map_mutex);
        if (file_mutex_map.find(file_name) == file_mutex_map.end()) {
            file_mutex_map[file_name] = std::make_unique<std::mutex>();
        }
        return Status::OK;
    }


    Status CallbackList(ServerContext *context, 
            const RequestFile *request_file, FileList *file_list) override {
        Void *void_ = NULL;
        return this->ListFiles(context, void_, file_list);
    }


    Status DeleteFile(ServerContext *context, 
            const RequestFile *request_file, FileInfo *return_file_info) override {
        std::string file_name = request_file->name();
        std::string client_id = request_file->request_client_id();
        //long mdf_time = request_file->request_mdf_time();
        //long client_crc = request_file->client_file_crc();
        std::string file_path = WrapPath(file_name);   

        /* Ensure the file has corresponding write lock */
        file_mutex_map_mutex.lock();
        if (file_mutex_map.find(file_name) == file_mutex_map.end()) {
            file_mutex_map[file_name] = std::make_unique<std::mutex>();
        }
        auto file_mutex_iter = file_mutex_map.find(file_name);
        std::mutex *file_mutex = file_mutex_iter->second.get();
        file_mutex_map_mutex.unlock();

        /* Check if the file has been owned by a client */
        file_client_map_mutex.lock();
        auto file_client_iter = file_client_map.find(file_name);
        if ((file_client_iter == file_client_map.end()) || ((file_client_iter->second).compare(client_id) != 0)) {
            std::stringstream str_str;
            str_str << client_id << " has no write lock for " << file_name << ", or the file has already been locked" << std::endl;
            dfs_log(LL_SYSINFO) << str_str.str();
            file_client_map_mutex.unlock();
            return Status(StatusCode::INTERNAL, str_str.str());
        }
        file_client_map_mutex.unlock();

        std::lock_guard<std::mutex> lock3(dir_mutex);
        std::lock_guard<std::mutex> lock4(*file_mutex);
        
        /* Check if file exists */
        struct stat st;
        if (stat(file_path.c_str(), &st) == -1) {
            std::stringstream str_stream;
            str_stream << "File not found for " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();

            std::lock_guard<std::mutex> lock(file_client_map_mutex);
            file_client_map.erase(file_name);
            return Status(StatusCode::NOT_FOUND, str_stream.str());
        } 

        /* Delete the file */
        int rv = remove(file_path.c_str());
        if (rv == -1) {
            std::stringstream str_stream;
            str_stream << "Server fail to delete " << file_path;
            dfs_log(LL_ERROR) << str_stream.str();

            std::lock_guard<std::mutex> lock(file_client_map_mutex);
            file_client_map.erase(file_name);
            return Status(StatusCode::INTERNAL, str_stream.str());            
        }

        dfs_log(LL_SYSINFO) << "Server sucessfully deleted the file " << file_name;

        return_file_info->set_name(file_name);
        long mdf_time_2 = static_cast<long> (st.st_mtim.tv_sec);
        return_file_info->set_mdf_time(mdf_time_2); 

        std::lock_guard<std::mutex> lock5(file_client_map_mutex);
        file_client_map.erase(file_name);
        return Status::OK;        
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}


//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
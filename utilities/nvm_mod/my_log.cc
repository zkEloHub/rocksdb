#include "my_log.h"
namespace rocksdb {
std::string log_file0;
void init_log_file() {
  FILE* fp;
#ifdef LZW_INFO
  fp = fopen(log_file1.c_str(), "w");
  if (fp == nullptr) printf("log failed\n");
  fclose(fp);

  fp = fopen(log_file2.c_str(), "w");
  if (fp == nullptr) printf("log failed\n");
  fclose(fp);

  fp = fopen(log_file3.c_str(), "w");
  if(fp == nullptr) printf("log failed\n");
  fclose(fp);
  RECORD_INFO(3,"compaction,read(MB),write(MB),time(s),start(s),is_level0\n");

  fp = fopen(log_file4.c_str(), "w");
  if(fp == nullptr) printf("log failed\n");
  fclose(fp);

  fp = fopen(log_file5.c_str(), "w");
  if(fp == nullptr) printf("log failed\n");
  fclose(fp);
  RECORD_INFO(5,"now(s),through(iops),p90,,,p99,,,p999,,,p9999,,,p99999,,,\n");

#endif

#ifdef LZW_DEBUG
  fp = fopen(log_file0.c_str(), "w");
  if (fp == nullptr) printf("log failed\n");
  fclose(fp);

#endif


}

void LZW_LOG(int file_num, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  char buf[8192];
  vsprintf(buf, format, ap);
  va_end(ap);

  const std::string* log_file;
  switch (file_num) {
    case 0:
      if (log_file0.size() == 0) {
        log_file0 = "NVM_LOG";
      }
      log_file = &log_file0;
      break;
    case 1:
      log_file = &log_file1;
      break;
    case 2:
      log_file = &log_file2;
      break;
    case 3:
      log_file = &log_file3;
      break;
    case 4:
      log_file = &log_file4;
      break;
    case 5:
      log_file = &log_file5;
      break;
    default:
      return;
  }

  FILE* fp = fopen(log_file->c_str(), "a");
  if (fp == nullptr) printf("log failed\n");
  fprintf(fp, "%s", buf);
  fclose(fp);
}
// /mnt/server_0/0
void init_nvm_log(const std::string& pmem_path)
{
  std::size_t found = pmem_path.find_last_of("/");
  std::string str_next = pmem_path.substr(found + 1);
  std::string str_pre = pmem_path.substr(0, found);
  found = str_pre.find_last_of("/");
  log_file0 = str_pre.substr(found + 1) + "_" + str_next + "_NVM_LOG";
}

}  // namespace rocksdb
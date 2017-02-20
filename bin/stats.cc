#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <fstream>
#include <ctype.h>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <set>
#include <algorithm>

std::unordered_map<uint64_t, std::set<uint32_t>> ops;
std::vector<uint32_t> counts;

uint32_t max = 0;
double sum = 0.0;

void print_usage(const char* exec) {
  fprintf(stderr, "Usage: %s operations-file\n", exec);
}

void process_ops_file(std::ifstream& in, size_t op_cnt) {
  uint64_t node_id;
  uint32_t shard_id;
  char sep, type;
  uint64_t num_ops = 0;
  while (in >> node_id >> sep >> shard_id >> type && sep == ',' && num_ops < op_cnt) {
    ops[node_id].insert(shard_id);
    num_ops++;
  }
  fprintf(stderr, "Processed %llu ops; ", num_ops);
  
  for (auto op: ops) {
    uint32_t count = op.second.size();
    if (op.first < 10000001)
      counts.push_back(count + 1);
    else
      counts.push_back(count);
    sum += counts.back();
    if (counts.back() > max)
      max = counts.back();
  }
  std::sort(counts.begin(), counts.end());

  fprintf(stderr, "Max = %u, Mean = %lf\n", max, sum / ops.size());
}

void output_stats(const std::string& file, uint64_t op_cnt) {
  std::string out_prefix = file + "." + std::to_string(op_cnt);
  std::ofstream mean_out(out_prefix + ".mean");
  mean_out << sum / ops.size() << "\n";
  mean_out.close();

  std::ofstream max_out(out_prefix + ".max");
  max_out << max << "\n";
  max_out.close();

  std::ofstream count_out(out_prefix + ".count");
  for (auto count : counts)
    count_out << count << "\n";
  count_out.close();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    print_usage(argv[0]);
    return 0;
  }

  std::string file = std::string(argv[1]);
  std::ifstream in(file);
  for (uint64_t i = 1; i <= 10; i++) {
    process_ops_file(in, 100000000ULL);
    output_stats(file, i * 100000000ULL);
  }

  return 0;
}

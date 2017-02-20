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
#include <unistd.h>

std::unordered_map<uint64_t, std::set<int32_t>> ops;

uint32_t cdf[101];
uint32_t max = 0;
double sum = 0.0;

void print_usage(const char* exec) {
  fprintf(stderr, "Usage: %s [OPTIONS] operations-file\n\n", exec);
  fprintf(stderr, "OPTIONS:\n");
  fprintf(stderr, "\t-i Intervals for computing statistics\n");
  fprintf(stderr, "\t-n Number of nodes initially present in database\n");
}

void init_ops(uint64_t init_nodes) {
  for (uint64_t i = 1; i <= init_nodes; i++)
    ops[i].insert(-1);
}

void process_ops_file(std::ifstream& in, size_t op_cnt) {
  uint64_t node_id;
  int32_t shard_id;
  char sep, type;
  uint64_t num_ops = 0;
  while (in >> node_id >> sep >> shard_id >> type && sep == ',' && num_ops < op_cnt) {
    ops[node_id].insert(shard_id);
    num_ops++;
  }
  fprintf(stderr, "Processed %llu ops; ", num_ops);
  
  std::vector<uint32_t> counts;
  for (auto op: ops) {
    counts.push_back(op.second.size());
    sum += counts.back();
  }
  
  std::sort(counts.begin(), counts.end());
  for (size_t i = 0; i < 100; i++) {
    double mark = ((double) (i * counts.size())) / 100.0;
    cdf[i] = counts[(size_t) mark];
  }
  max = cdf[100] = counts.back();

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

  std::ofstream cdf_out(out_prefix + ".cdf");
  for (size_t i = 0; i <= 100; i++)
    cdf_out << cdf[i] << "\t" << i << "\n";
  cdf_out.close();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    print_usage(argv[0]);
    return 0;
  }

  int c;
  uint64_t op_interval = 100000000ULL;
  uint64_t init_nodes = 10000000ULL;

  while ((c = getopt(argc, argv, "i:n:")) != -1) {
    switch(c) {
      case 'i':
        op_interval = std::stoll(std::string(optarg));
        break;
      case 'x':
        init_nodes = std::stoll(std::string(optarg));
        break;
    }
  }

  init_ops(init_nodes);
  std::string file = std::string(argv[1]);
  std::ifstream in(file);
  for (uint64_t i = 1; in && in.peek() != EOF; i++) {
    process_ops_file(in, op_interval);
    output_stats(file, i * op_interval);
  }
  in.close();

  return 0;
}

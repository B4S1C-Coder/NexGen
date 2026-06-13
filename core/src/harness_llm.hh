#pragma once

#include "llama.h"
#include <vector>
#include <string>
#include "logger.hh"

// Temporarily set here. Default for Qwen-3.5-4B
struct HarnessConfig {
  int n_ctx        = 65536;
  int n_batch      = 512;
  int n_gpu_layers = 99;
  int n_seq_max    = 2; // seq_id=0 and seq_id=1
  int n_threads    = 8;

  llama_flash_attn_type flash_attn = LLAMA_FLASH_ATTN_TYPE_AUTO;
  
  ggml_type type_k = GGML_TYPE_Q4_0;
  ggml_type type_v = GGML_TYPE_Q4_0;
  bool offload_kqv = true;  // offload kqv ops to GPU
  bool kv_unified  = false; // disable for n_seq_max > 1 but we could try enabling
                            // it since our sequences do share a large prefix
  float temp       = 0.7f;
  float top_p      = 0.9f;
  int top_k        = 40;
  int seed         = 42;
};

class HarnessLLM {
private:
  llama_model* model       = nullptr;
  llama_context* ctx       = nullptr;
  llama_sampler* sampler   = nullptr;
  const llama_vocab* vocab = nullptr;

  HarnessConfig cfg_;

  int static_prefix_end = 0; // token pos where seq_id=0 ends
  int dynamic_end       = 0; // current end of seq_id=1 content

  std::vector<uint8_t> prefix_state_blob;
  std::vector<llama_token> prefix_tokens;

  std::vector<llama_token> tokenize(
    const std::string& text,
    bool add_bos
  ) const;

  bool decode_tokens(
    const std::vector<llama_token>& tokens,
    int start_pos,
    int seq_id,
    bool logits_last
  );

public:
  // Lifecycle
  bool init(
    const std::string& model_path,
    const HarnessConfig& cfg
  );

  void shutdown();

  // Static prefix (called once at session start)
  bool ingest_static_prefix(
    const std::string& system_prompt,
    const std::string& tool_defs
  );

  // Per phase ops
  void reset_to_prefix();
  bool ingest_dynamic(const std::string& text);
  // std::string generate(
  //   int max_tokens,
  //   float temp
  // );

  std::string generate(int max_tokens);

  // State Persistance
  std::vector<uint8_t> save_state() const;
  bool restore_state(const std::vector<uint8_t>& blob);

  // Helpers
  int count_tokens(const std::string& text) const;
  int context_tokens_used() const { return dynamic_end; }
  int context_tokens_left() const { return cfg_.n_ctx - dynamic_end; }
};
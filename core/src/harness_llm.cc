#include "harness_llm.hh"

bool HarnessLLM::init(
  const std::string& model_path,
  const HarnessConfig& cfg
) {
  
  cfg_ = cfg;

  llama_backend_init();

  llama_model_params mparams = llama_model_default_params();
  mparams.n_gpu_layers = cfg_.n_gpu_layers;

  model = llama_model_load_from_file(model_path.c_str(), mparams);
  if (!model) {
    LOG_ERROR("Failed to load model: {}", model_path.c_str());
    return false;
  }

  LOG_INFO("Model loaded: {}", model_path.c_str());

  vocab = llama_model_get_vocab(model);

  llama_context_params cparams = llama_context_default_params();
  cparams.n_ctx           = (uint32_t)cfg_.n_ctx;
  cparams.n_batch         = (uint32_t)cfg_.n_batch;
  cparams.n_ubatch        = (uint32_t)cfg_.n_batch;
  cparams.n_seq_max       = (uint32_t)cfg_.n_seq_max;
  cparams.n_threads       = cfg_.n_threads;
  cparams.flash_attn_type = cfg_.flash_attn;
  cparams.type_k          = cfg_.type_k;
  cparams.type_v          = cfg_.type_v;
  cparams.offload_kqv     = cfg_.offload_kqv;
  cparams.kv_unified      = cfg_.kv_unified;

  ctx = llama_init_from_model(model, cparams);

  if (!ctx) {
    LOG_ERROR("Failed to create context");
    llama_model_free(model);
    model = nullptr;
    return false;
  }

  LOG_INFO("Context created");

  sampler = llama_sampler_chain_init(llama_sampler_chain_default_params());
  llama_sampler_chain_add(sampler, llama_sampler_init_top_k(cfg_.top_k));
  llama_sampler_chain_add(sampler, llama_sampler_init_top_p(cfg_.top_p, 1));
  llama_sampler_chain_add(sampler, llama_sampler_init_temp(cfg_.temp));
  llama_sampler_chain_add(sampler, llama_sampler_init_dist(cfg_.seed));

  LOG_INFO("init OK - n_ctx = {} n_seq_max = {}", cfg_.n_ctx, cfg_.n_seq_max);

  return true;
}

void HarnessLLM::shutdown() {
  LOG_INFO("Shutting Down ...");
  if (sampler) { llama_sampler_free(sampler); sampler = nullptr; }
  if (ctx)     { llama_free(ctx); ctx = nullptr; }
  if (model)   { llama_model_free(model); model = nullptr; }
  llama_backend_free();
}

bool HarnessLLM::ingest_static_prefix(
  const std::string& system_prompt,
  const std::string& tool_defs
) {

  std::string text = system_prompt + tool_defs;
  prefix_tokens = tokenize(text, true);

  if ((int)prefix_tokens.size() >= cfg_.n_ctx) {
    LOG_ERROR("Prefix too long: {} tokens (max {})", prefix_tokens.size(), cfg_.n_ctx);
    return false;
  }

  if (!decode_tokens(prefix_tokens, 0, 0, false)) return false;

  static_prefix_end = (int)prefix_tokens.size();
  dynamic_end = static_prefix_end;

  size_t sz = llama_state_get_size(ctx);
  prefix_state_blob.resize(sz);

  size_t written = llama_state_get_data(ctx, prefix_state_blob.data(), sz);
  prefix_state_blob.resize(written);

  LOG_INFO(
    "Static prefix ingested -- tokens: {} | state blob: {:.1f} MiB",
    static_prefix_end,
    (float)prefix_state_blob.size() / (1024.0f * 1024.0f)
  );

  return true;
}

void HarnessLLM::reset_to_prefix() {
  if (prefix_state_blob.empty()) {
    LOG_ERROR("reset_to_prefix called but no prefix state saved");
    return;
  }

  llama_state_set_data(ctx, prefix_state_blob.data(), prefix_state_blob.size());
  dynamic_end = static_prefix_end;
  llama_sampler_reset(sampler);
}

bool HarnessLLM::ingest_dynamic(const std::string& text) {
  if (text.empty()) return true;

  auto tokens = tokenize(text, false);

  if (dynamic_end + (int)tokens.size() >= cfg_.n_ctx) {
    LOG_ERROR(
      "Context full -- used: {} incoming: {} capacity: {}",
      dynamic_end,
      tokens.size(),
      cfg_.n_ctx
    );
    return false;
  }

  if (!decode_tokens(tokens, dynamic_end, 1, true)) return false;

  dynamic_end += (int)tokens.size();
  return true;
}

std::string HarnessLLM::generate(int max_tokens) {
  if (prefix_state_blob.empty()) {
    LOG_ERROR("generate called before ingest_static_prefix");
    return "";
  }

  // temp override: swap the temp sampler in the chain
  // chain order is: top_k -> top_p -> temp -> dist
  // index 2 is temp - replace it for this call
  // llama_sampler_chain_remove(sampler, 2);
  // llama_sampler_chain_add(sampler, llama_sampler_init_temp(temp));

  std::string result;
  int cur_pos = dynamic_end;

  for (int i = 0; i < max_tokens; i++) {
    llama_token tok = llama_sampler_sample(sampler, ctx, -1);

    if (llama_vocab_is_eog(vocab, tok)) break;

    char buf[256];
    int len = llama_token_to_piece(vocab, tok, buf, sizeof(buf), 0, true);
    if (len > 0) result.append(buf, len);

    llama_batch batch  = llama_batch_init(1, 0, 1);
    batch.token[0]     = tok;
    batch.pos[0]       = cur_pos++;
    batch.n_seq_id[0]  = 1;
    batch.seq_id[0][0] = 1;
    batch.logits[0]    = true;
    batch.n_tokens     = 1;

    int ret = llama_decode(ctx, batch);
    llama_batch_free(batch);

    if (ret != 0) {
      LOG_ERROR("llama_decode failed in generate at token {}: ret={}", i, ret);
      break;
    }
  }

  dynamic_end = cur_pos;
  return result;
}

std::vector<uint8_t> HarnessLLM::save_state() const {
  size_t sz = llama_state_get_size(ctx);
  std::vector<uint8_t> blob(sz);
  size_t written = llama_state_get_data(ctx, blob.data(), sz);
  blob.resize(written);
  return blob;
}

bool HarnessLLM::restore_state(const std::vector<uint8_t>& blob) {
  if (blob.empty()) {
    LOG_ERROR("restore_state called with empty blob");
    return false;
  }
  size_t consumed = llama_state_set_data(ctx, blob.data(), blob.size());
  if (consumed == 0) {
    LOG_ERROR("restore_state failed — context params likely don't match saved state");
    return false;
  }
  llama_sampler_reset(sampler);
  return true;
}

int HarnessLLM::count_tokens(const std::string& text) const {
  return (int)tokenize(text, false).size();
}

std::vector<llama_token> HarnessLLM::tokenize(
  const std::string& text,
  bool add_bos
) const {
  int n = -llama_tokenize(vocab,
              text.c_str(), (int32_t)text.size(),
              nullptr, 0,
              add_bos, /*special=*/true);
  if (n <= 0) return {};

  std::vector<llama_token> out(n);
  llama_tokenize(vocab,
      text.c_str(), (int32_t)text.size(),
      out.data(), n,
      add_bos, true);
  return out;
}

bool HarnessLLM::decode_tokens(
  const std::vector<llama_token>& tokens,
  int start_pos, int seq_id, bool logits_last
) {
  if (tokens.empty()) return true;

  const int chunk_size = cfg_.n_batch;
  int i = 0;

  while (i < (int)tokens.size()) {
    int  chunk         = std::min(chunk_size, (int)tokens.size() - i);
    bool is_last_chunk = (i + chunk == (int)tokens.size());

    llama_batch batch = llama_batch_init(chunk, 0, 1);
    for (int j = 0; j < chunk; j++) {
      batch.token[j]     = tokens[i + j];
      batch.pos[j]       = start_pos + i + j;
      batch.n_seq_id[j]  = 1;
      batch.seq_id[j][0] = seq_id;
      batch.logits[j]    = logits_last && is_last_chunk && (j == chunk - 1);
    }
    batch.n_tokens = chunk;

    int ret = llama_decode(ctx, batch);
    llama_batch_free(batch);

    if (ret != 0) {
      LOG_ERROR("llama_decode failed — chunk {}/{} seq_id={} ret={}",
                i / chunk_size,
                (tokens.size() + chunk_size - 1) / chunk_size,
                seq_id, ret);
      return false;
    }
    i += chunk;
  }
  return true;
}
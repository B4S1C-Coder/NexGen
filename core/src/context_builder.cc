#include "context_builder.hh"
#include <sstream>

void ContextBuilder::set_budget(const HarnessLLM& llm, int generation_headroom) {
  // context_tokens_left() = n_ctx - dynamic_end
  // after ingest_static_prefix, dynamic end = static_prefix_end
  // so this is exactly how many tokens remain for seq_id=1 content + generation
  token_budget_ = llm.context_tokens_left() - generation_headroom;

  if (token_budget_ <= 0)
    LOG_ERROR(
      "No token budget left after static prefix and generation headroom ({})",
      generation_headroom
    );
  else
    LOG_INFO("token budget = {} tokens", token_budget_);
}

bool ContextBuilder::build(std::string& out, const HarnessLLM& llm) const {
  if (token_budget_ <= 0) {
    LOG_ERROR("build called with no budget set - call set_budget() after ingest_static_prefix()");
    return false;
  }

  // wroking copies (can be truncated)
  std::string memory   = memory_keys_;
  std::string fetched  = fetched_context_;
  std::string convo    = active_conversation_;
  std::string output_z = output_zone_;

  // token counts for each regiob
  int t_memory = llm.count_tokens(memory);
  int t_output_z = llm.count_tokens(output_z);

  // memory keys and output zone are required and assumed small
  // if they are already exceeding budget, something is very wrong
  if (t_memory + t_output_z > token_budget_) {
    LOG_ERROR(
      "Required regions (memory_keys={} + output_zone={}) already exceed budget ({})",
      t_memory,
      t_output_z,
      token_budget_
    );
    return false;
  }

  int remaining = token_budget_ - t_memory - t_output_z;

  // fetched_context: truncate from back
  // drop the tail, fetched context usually contains retrieved docs / tool results,
  // the most relevant parts tend to be at the front
  int t_fetched = llm.count_tokens(fetched);
  if (t_fetched > remaining) {
    LOG_WARN(
      "fetched_context too large ({} tokens), truncating to fit {} tokens",
      t_fetched,
      remaining
    );

    fetched = truncate_back(fetched, remaining / 2, llm);
    t_fetched = llm.count_tokens(fetched);
  }

  remaining -= t_fetched;

  // active conversation: truncate from front
  // drop oldest turns first - front of conversation is oldest
  int t_convo = llm.count_tokens(convo);
  if (t_convo > remaining) {
    LOG_INFO(
      "active_conversation too large ({} tokens), truncating oldest turns to fit {} tokens",
      t_convo,
      remaining
    );
    convo = truncate_front(convo, remaining, llm);
    t_convo = llm.count_tokens(convo);
  }

  remaining -= t_convo;

  if (remaining < 0) {
    LOG_ERROR("Budget exceeded even after truncation");
    return false;
  }

  // assemble in order
  // [ memory_keys | fetched_context | conversation | output_zone ]
  std::ostringstream ss;
  if (!memory.empty()) ss << memory << "\n";
  if (!fetched.empty()) ss << fetched << "\n";
  if (!convo.empty()) ss << convo << "\n";
  if (!output_z.empty()) ss << output_z;

  out = ss.str();

  LOG_INFO(
    "built dynamic context -- memory={} fetched={} convo={} output_zone={} total=~{} tokens",
    t_memory,
    t_fetched,
    t_convo,
    t_output_z,
    t_memory + t_fetched + t_convo + t_output_z
  );

  return true;
}

void ContextBuilder::reset() {
  memory_keys_.clear();
  fetched_context_.clear();
  active_conversation_.clear();
  output_zone_.clear();

  // DO NOT RESET token_budget_, it stays valid as long as the static prefix hasn't changed
}

int ContextBuilder::total_tokens(const HarnessLLM& llm) const {
  return llm.count_tokens(memory_keys_)
    + llm.count_tokens(fetched_context_)
    + llm.count_tokens(active_conversation_)
    + llm.count_tokens(output_zone_);
}

void ContextBuilder::print_budget(const HarnessLLM& llm) const {
  int t_memory  = llm.count_tokens(memory_keys_);
  int t_fetched = llm.count_tokens(fetched_context_);
  int t_convo   = llm.count_tokens(active_conversation_);
  int t_output  = llm.count_tokens(output_zone_);
  int total     = t_memory + t_fetched + t_convo + t_output;

  LOG_INFO("ContextBuilder budget breakdown:");
  LOG_INFO("  budget        : {} tokens", token_budget_);
  LOG_INFO("  memory_keys   : {} tokens", t_memory);
  LOG_INFO("  fetched_ctx   : {} tokens", t_fetched);
  LOG_INFO("  conversation  : {} tokens", t_convo);
  LOG_INFO("  output_zone   : {} tokens", t_output);
  LOG_INFO("  total dynamic : {} tokens ({} remaining)",
           total, token_budget_ - total);
}

// Truncation uses binary search on character position. It is coarse but avoids
// Re-Tokenizing the entire string every iteration

std::string ContextBuilder::truncate_back(const std::string& text, int max_tokens, const HarnessLLM& llm) const {
  if (max_tokens <= 0) return "";

  int lo = 0, hi = (int)text.size();
  while (lo < hi) {
    int mid = (lo + hi + 1) / 2;
    std::string candidate = text.substr(0, mid);

    if (llm.count_tokens(candidate) <= max_tokens) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }

  return text.substr(0, lo);
}

std::string ContextBuilder::truncate_front(const std::string& text, int max_tokens, const HarnessLLM& llm) const {
  if (max_tokens <= 0) return "";

  int lo = 0, hi = (int)text.size();
  while (lo < hi) {
    int mid = (lo + hi) / 2;
    std::string candidate = text.substr(mid);

    if (llm.count_tokens(candidate) <= max_tokens) {
      hi = mid;
    } else {
      lo = mid + 1;
    }
  }

  return text.substr(lo);
}
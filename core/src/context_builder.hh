#pragma once

#include <string>
#include <vector>
#include "harness_llm.hh"
#include "logger.hh"

// Represents one named region of the dynamic context window (seq_id=1).
// Regions are assembled in insertion order.
struct ContextRegion {
  std::string name;
  std::string content;
  bool required;       // true  -> build() fails if this region doesn't fit
                       // false -> it gets dropped when over budget
};

class ContextBuilder {

public:
  // Regions - set individually before build()
  // memory keys -> fetched context -> active conversation -> output

  void set_memory_keys         (const std::string& text) { memory_keys_ = text; }
  void set_fetched_context     (const std::string& text) { fetched_context_ = text; }
  void set_active_conversation (const std::string& text) { active_conversation_ = text; }
  void set_output_zone         (const std::string& text) { output_zone_ = text; }

  // Budget
  // To be called right after HarnessLLM::ingest_static_prefix() to set how
  // many tokens are available for dynamic portion.
  // Leave headroom for generated output.
  void set_budget(const HarnessLLM& llm, int generation_headroom = 512);

  // Build
  // Assemble all regions in order into `out`.
  // Truncate when order budget:
  //    1. truncate fetched_context (biggest, least critical to preserve exactly)
  //    2. truncate active_conversation (drop oldest turns from the front)
  //    3. fail if still over budget
  bool build(std::string& out, const HarnessLLM& llm) const;

  void print_budget(const HarnessLLM& llm) const;
  int  total_tokens(const HarnessLLM& llm) const;

  // reset all regions for next phase
  void reset();

private:
  std::string memory_keys_;
  std::string fetched_context_;
  std::string active_conversation_;
  std::string output_zone_;

  // tokens available for seq_id=1
  int token_budget_ = 0;

  // truncate text from the front (dropping oldest content) to fit in max_tokens
  std::string truncate_front(
    const std::string& text,
    int max_tokens,
    const HarnessLLM& llm
  ) const;

  // truncate text from the back to fit in max_tokens
  std::string truncate_back(
    const std::string& text,
    int max_tokens,
    const HarnessLLM& llm
  ) const;
};
#include "harness_llm.hh"
#include "logger.hh"

#include <cassert>
#include <iostream>

int main() {
    Logger::set_level(LogLevel::Debug);

    HarnessConfig cfg;
    cfg.n_ctx        = 4096;
    cfg.n_batch      = 256;
    cfg.n_gpu_layers = 99;
    cfg.n_threads    = 8;

    HarnessLLM llm;

    const std::string model_path =
        "/home/saksham/codebase/nexgen/models/Qwen3.5-4B-Q4_K_M.gguf";

    LOG_INFO("=== TEST: init ===");

    bool ok = llm.init(model_path, cfg);
    assert(ok);

    LOG_INFO("=== TEST: token counting ===");

    int token_count =
        llm.count_tokens("Hello world");

    LOG_INFO("Token count = {}", token_count);

    assert(token_count > 0);

    LOG_INFO("=== TEST: ingest_static_prefix ===");

    ok = llm.ingest_static_prefix(
        R"(You are a helpful assistant.)",
        R"(
Available tools:
- search
- calculator
)"
    );

    assert(ok);

    int prefix_tokens =
        llm.context_tokens_used();

    LOG_INFO(
        "Context used after prefix = {}",
        prefix_tokens
    );

    assert(prefix_tokens > 0);

    LOG_INFO("=== TEST: save_state ===");

    auto prefix_state =
        llm.save_state();

    assert(!prefix_state.empty());

    LOG_INFO(
        "Saved state size = {:.2f} MiB",
        static_cast<double>(prefix_state.size())
            / (1024.0 * 1024.0)
    );

    LOG_INFO("=== TEST: ingest_dynamic ===");

    ok = llm.ingest_dynamic(
        "What is the capital of France?\nAssistant:"
    );

    assert(ok);

    int dynamic_tokens =
        llm.context_tokens_used();

    LOG_INFO(
        "Context used after dynamic = {}",
        dynamic_tokens
    );

    assert(dynamic_tokens > prefix_tokens);

    LOG_INFO("=== TEST: generation ===");

    std::string response =
        llm.generate(32);

    std::cout
        << "\n=== RESPONSE ===\n"
        << response
        << "\n================\n";

    assert(!response.empty());

    int after_generation =
        llm.context_tokens_used();

    assert(after_generation > dynamic_tokens);

    LOG_INFO("=== TEST: restore_state ===");

    ok = llm.restore_state(prefix_state);

    assert(ok);

    LOG_INFO("State restored successfully");

    LOG_INFO("=== TEST: reset_to_prefix ===");

    llm.reset_to_prefix();

    int after_reset =
        llm.context_tokens_used();

    LOG_INFO(
        "Context used after reset = {}",
        after_reset
    );

    assert(after_reset == prefix_tokens);

    LOG_INFO("=== TEST: second generation ===");

    ok = llm.ingest_dynamic(
        "Respond with exactly one word.\nAssistant:"
    );

    assert(ok);

    response =
        llm.generate(8);

    std::cout
        << "\n=== SECOND RESPONSE ===\n"
        << response
        << "\n=======================\n";

    assert(!response.empty());

    LOG_INFO("=== TEST: repeated save/restore ===");

    for (int i = 0; i < 10; ++i) {
        auto state = llm.save_state();

        assert(!state.empty());

        bool restored =
            llm.restore_state(state);

        assert(restored);
    }

    LOG_INFO("Repeated save/restore passed");

    LOG_INFO("=== TEST: repeated generation ===");

    for (int i = 0; i < 5; ++i) {
        llm.reset_to_prefix();

        ok = llm.ingest_dynamic(
            "Say hello.\nAssistant:"
        );

        assert(ok);

        auto text =
            llm.generate(4);

        LOG_DEBUG(
            "Iteration {} => '{}'",
            i,
            text
        );
    }

    LOG_INFO("=== SHUTDOWN ===");

    llm.shutdown();

    LOG_INFO("ALL TESTS PASSED");

    return 0;
}
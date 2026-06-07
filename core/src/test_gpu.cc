#include <cstdio>
#include "llama.h"
#include "ggml.h"
#include "ggml-backend.h"

int main() {
  llama_backend_init();

  // print build info
  // printf("llama.cpp build: %d\n", llama_build_number());

  // list devices — this shows GPU detection
  int n_devices = ggml_backend_dev_count();
  printf("devices found: %d\n", n_devices);

  for (int i = 0; i < n_devices; i++) {
    ggml_backend_dev_t dev = ggml_backend_dev_get(i);
    printf("  [%d] %s | type: %d\n", i,
            ggml_backend_dev_name(dev),
            (int)ggml_backend_dev_type(dev));
  }

  // try loading your model
  const char* model_path = "/home/saksham/codebase/nexgen/models/Qwen3.5-4B-Q4_K_M.gguf";

  llama_model_params mparams = llama_model_default_params();
  mparams.n_gpu_layers = 99;

  llama_model* model = llama_model_load_from_file(model_path, mparams);
  if (!model) {
    printf("FAILED to load model\n");
    llama_backend_free();
    return 1;
  }

  printf("model loaded OK\n");

  // quick context creation to confirm GPU context works
  llama_context_params cparams = llama_context_default_params();
  cparams.n_ctx = 512;

  llama_context* ctx = llama_init_from_model(model, cparams);
  if (!ctx) {
    printf("FAILED to create context\n");
    llama_model_free(model);
    llama_backend_free();
    return 1;
  }

  printf("context created OK\n");
  // printf("GPU layers: %d\n", llama_model_n_gpu_layers(model));

  llama_free(ctx);
  llama_model_free(model);
  llama_backend_free();

  printf("all good\n");
  return 0;
}
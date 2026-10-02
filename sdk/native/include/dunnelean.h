#ifndef DUNNELEAN_NATIVE_H
#define DUNNELEAN_NATIVE_H
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DUNNELEAN_ABI_VERSION 1u
#define DUNNELEAN_OP_VALIDATE 1u
#define DUNNELEAN_OP_SUBMIT 2u
#define DUNNELEAN_OP_GET_RUN 3u
#define DUNNELEAN_OP_CANCEL_RUN 4u
#define DUNNELEAN_OP_LIST_RUNS 5u
#define DUNNELEAN_OP_GET_REQUEST 6u
#define DUNNELEAN_OP_CANCEL_REQUEST 7u
#define DUNNELEAN_OP_LIST_BATCHES 8u
#define DUNNELEAN_OP_STATE_STORE_ID 9u
#define DUNNELEAN_OP_READY 10u
#define DUNNELEAN_OP_CONNECTORS 11u

/*
 * Inputs are borrowed UTF-8 bytes for the duration of the call only.
 * Output pairs must be non-null, writable and non-overlapping.
 * Status 0 returns successful JSON. Status 1 (ordinary error) and 2 (caught
 * Rust panic) return JSON with code/message/commit_unknown/retryable.
 * Copy each output before freeing it exactly once with buffer_free.
 * Keep this library loaded while the process is running.
 */
uint32_t dunnelean_abi_version(void);
int32_t dunnelean_native_version(uint8_t **output, size_t *output_len);

/* Required state_path, optional max_running=2/max_queued=16.
 * Success: out_handle plus {"state_store_id":"..."}.
 * Failure: out_handle is zero. No pointers are retained.
 */
int32_t dunnelean_engine_open(const uint8_t *input, size_t input_len,
                             uint64_t *out_handle,
                             uint8_t **output, size_t *output_len);

/* validate/submit input is RunSpec JSON.
 * ID operations use {"id":"..."}.
 * list_runs uses {"limit":50,"offset":0}; lists return JSON arrays.
 * state_store_id returns a JSON string; ready returns {"status":"ready"}.
 * connectors accepts handle zero, with empty input.
 */
int32_t dunnelean_engine_invoke(uint64_t handle, uint32_t op,
                               const uint8_t *input, size_t input_len,
                               uint8_t **output, size_t *output_len);

/* Result {"closed":false} retains resources and supports query/cancel/retry.
 * Result {"closed":true} has released the state-store lock.
 * Closed handles remain valid for repeated close until release.
 */
int32_t dunnelean_engine_close(uint64_t handle, uint64_t timeout_ms,
                              uint8_t **output, size_t *output_len);

/* Idempotent release; any necessary cleanup is scheduled in the background.
 * Calls already executing retain ownership safely.
 */
int32_t dunnelean_engine_release(uint64_t handle);

/* Only free the exact pair returned by this library, once. NULL/0 is safe. */
void dunnelean_buffer_free(uint8_t *data, size_t len);

#ifdef __cplusplus
}
#endif
#endif

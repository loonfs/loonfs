#ifndef LOON_MACOS_H
#define LOON_MACOS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

char *loon_file_provider_bridge_open(const char *request_json);
char *loon_file_provider_bridge_close(const char *request_json);
char *loon_file_provider_bridge_list_root(const char *request_json);
char *loon_file_provider_bridge_lookup_item(const char *request_json);
char *loon_file_provider_bridge_list_children(const char *request_json);
char *loon_file_provider_bridge_materialize_item(const char *request_json);
void loon_file_provider_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif

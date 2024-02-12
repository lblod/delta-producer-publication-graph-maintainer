# Changelog
## 1.0.6 (2024-02-12)
- Fixing a couple of missing await statements and broken function calls which may result in silent errors
## 1.0.5 (2024-02-09)
- Fix broken function call with recursive typecache build
  - In case of conceptSchemeSelector this fixes the incomplete exports
## 1.0.4 (2024-01-09)
- Fix parsing of config data and its default settings. The boolean logic was incorrect, but this is hopefully(!) corrected now.
## 1.0.3 (2023-10-20)
- Bugfix on delete maintenance of triples
## 1.0.2 (2023-10-19)
- File based healing uses construct queries for better performance
## 1.0.1 (2023-09-21)
- Fixes with graphsfilter
## 1.0.0 (2023-07-13)

- This version introduces a new feature: file-based healing. This is primarily aimed at improving performance when dealing with large datasets.
- In addition, this version includes a range of corrections and bug fixes from the `v0.16.x` series.
  - If you're considering an upgrade from a version prior to `v0.16.x`, it's recommended to directly upgrade to `1.0.x`.
  - Please note, any breaking changes introduced in `v0.16.x` are still relevant in this version.
- Various performance tweaks.

## 0.16.x (2023-06-xx)

- **BREAKING CHANGE**: Configuration is now extracted from the environment variables of the Docker Compose setup.
     - You will need to update your configuration as per the instructions in readme.md.
     - The updated configuration requires only one container (eliminating the need for an additional container for each service).

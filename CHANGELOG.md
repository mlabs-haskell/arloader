# Changelog

All notable changes starting with v0.1.34 to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# v0.1.37 (2021-12-08)
- **added:** added [benchmarks](https://calebeverett.github.io/arloader/)

# v0.1.36 (2021-12-06)

- **changed:** refactored commands to only require wallets for upload transactions
- **added:** expansion of "~" in paths to home user directory

# v0.1.35 (2021-12-04)

- **changed:** more nits on the docs
- **changed:** alphabetized `arloader::commands`

# v0.1.34 (2021-12-04)

- **added:** `command_write_metaplex_items` to write links to json file formatted for use by metaplex candy machine to create NFTs
- **changed:** moved cli command functions from `main` to separate `commands` module
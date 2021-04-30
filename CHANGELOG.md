# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Type of Changes

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [Unreleased]

### Added

- [ ] Streaming AVCC NALUs
- [ ] Initial Facebook video streaming support, requiring custom ffmpeg-4.2.3 from NECLA-ML channel

### Fixed

- [ ] Best effort for KVS source
- [ ] Adaptive playback in renderer
- [ ] Some dash mpd urls cause parsing error: missing root node

## [0.5.5] - 2020-06-26

### Added

- [x] Add timestamp drifting threshold

### Changes

- [x] Generate KVS headers for cffi on the fly

## [0.5.1] - 2020-06-19

### Changed

- KVS-SDK-v3.0.0+ update

### Fixed

- Workaround to filter out trailing zero bytes in non-VCL NALUs

## [0.5.0] - 2020-06-07

### Added

- Dahua NVR RTSP streaming support

## Fixed

- Elimination of NALU trailing zero bytes
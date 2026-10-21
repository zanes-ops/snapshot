// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#include "model_streamer_transfer_engine.hpp"

#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <utility>

#include "transfer/filesystem/filesystem_storage.hpp"

namespace snapshot::pagebroker {
namespace {
[[noreturn]] void
RethrowS3TransferError(std::exception_ptr exception, Cancellation cancellation)
{
  try {
    std::rethrow_exception(exception);
  }
  catch (const TransferError&) {
    throw;
  }
  catch (const S3ResponseError&) {
    throw TransferError(Failure::ARTIFACT_CORRUPT, "invalid checkpoint object size");
  }
  catch (const CancellationError& error) {
    throw TransferError(TransferFailureCode(error), error.what());
  }
  catch (const S3Error& error) {
    const bool expired = cancellation.DeadlineExceeded() || error.primary.code == "DeadlineExceeded";
    throw TransferError(expired ? Failure::TRANSACTION_EXPIRED :
        error.primary.code == "Cancelled" ? Failure::INTERNAL_ERROR :
        error.primary.http_status == 401 || error.primary.http_status == 403 ? Failure::ACCESS_DENIED : Failure::STORAGE_UNAVAILABLE,
        "S3 checkpoint operation failed");
  }
  catch (const std::invalid_argument&) {
    throw TransferError(Failure::INVALID_REQUEST, "invalid checkpoint request or source");
  }
  catch (const std::exception&) {
    throw TransferError(Failure::STORAGE_ERROR, "checkpoint storage operation failed");
  }
}

bool
HasValidAddressingSetting(const std::string& addressing)
{
  if (!std::getenv("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING"))
    return true;
  return addressing == "0" || addressing == "1";
}

std::string
Environment(const char* name)
{
  const auto* value = std::getenv(name);
  return value ? value : "";
}

}  // namespace

ModelStreamerTransferEngine::ModelStreamerTransferEngine(S3Config config)
    : s3_options_(config.transfer), store_(std::move(config)), restore_(CreateRestore())
{
  ValidateS3Configuration();
  if (store_.config().store_id.empty() || store_.config().bucket.empty())
    throw std::invalid_argument("S3 checkpoint storage is not configured");
}

void
ModelStreamerTransferEngine::ValidateS3Configuration() const
{
  const auto& options = s3_options_;
  options.connection.Validate();
  options.upload_limits.Validate();
  if (options.restore_timeout <= std::chrono::milliseconds::zero() || options.restore_timeout > std::chrono::hours(24))
    throw std::invalid_argument("S3 restore timeout must be positive and at most 24 hours");

  const auto addressing = Environment("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING");
  if (!HasValidAddressingSetting(addressing))
    throw std::invalid_argument("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING must be 0 or 1");
  if (options.connection.use_virtual_addressing != (addressing != "0"))
    throw std::invalid_argument("S3 addressing must match RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING");
  if (options.connection.ca_file != Environment("AWS_CA_BUNDLE"))
    throw std::invalid_argument("S3 CA file must match AWS_CA_BUNDLE for both upload and restore");
}

const S3StorageBackend&
ModelStreamerTransferEngine::ArtifactStorage() const
{
  ValidateS3Configuration();
  return store_;
}

std::shared_ptr<ModelStreamerRestore>
ModelStreamerTransferEngine::CreateRestore() const
{
  const auto& connection = s3_options_.connection;
  return std::make_shared<ModelStreamerRestore>(
      ModelStreamerSessionOptions{connection.region, connection.endpoint, connection.access_key_id,
                                  connection.secret_access_key, connection.session_token},
      s3_options_.restore_timeout);
}

// Reuse a healthy coordinator; replace it after a terminal native failure.
// Existing callers keep the old wrapper until they finish cleanup.
std::shared_ptr<ModelStreamerRestore>
ModelStreamerTransferEngine::AcquireRestore() const
{
  for (;;) {
    std::shared_ptr<ModelStreamerRestore> current;
    {
      std::lock_guard lock(restore_mutex_);
      current = restore_;
    }
    if (!current->Failed())
      return current;

    auto replacement = CreateRestore();
    {
      std::lock_guard lock(restore_mutex_);
      if (restore_ == current) {
        restore_ = replacement;
        return replacement;
      }
    }
  }
}

IoEngine
ModelStreamerTransferEngine::type() const
{
  return IoEngine::MODEL_STREAMER;
}

RestoreAccess
ModelStreamerTransferEngine::StageRestore(PreparedRestore preparation, const Path& destination, Cancellation cancellation) const
{
  ValidateS3Configuration();
  try {
    cancellation.ThrowIfCancelled();
    if (preparation.mode != TransferMode::Staged)
      throw std::invalid_argument("S3 restore requires staging");
    if (!preparation.local_source.empty() || preparation.source.get() >= 0)
      throw std::invalid_argument("S3 restore cannot borrow a local source");
    AcquireRestore()->Stage(preparation.plan, destination, cancellation);
    cancellation.ThrowIfCancelled();
    return filesystem_storage::CompleteRestore(std::move(preparation), destination);
  }
  catch (...) {
    RethrowS3TransferError(std::current_exception(), cancellation);
  }
}

PublishedArtifact
ModelStreamerTransferEngine::ResolveArtifactTarget(const ArtifactTarget& target) const
{
  return ArtifactStorage().ResolveTarget(target);
}

void
ModelStreamerTransferEngine::ValidateArtifact(const PublishedArtifact& artifact) const
{
  ArtifactStorage().ValidateCheckpoint(artifact);
}

PreparedRestore
ModelStreamerTransferEngine::PrepareRestore(const StorageBackend& source, Cancellation cancellation,
    const PublishedArtifact* artifact, bool metadata_only, TransferMode mode) const
{
  if (!artifact || source.has_filesystem())
    throw std::invalid_argument("S3 restore requires an artifact source");
  try {
    return ArtifactStorage().PrepareRestore(*artifact, metadata_only, mode, cancellation);
  }
  catch (...) {
    RethrowS3TransferError(std::current_exception(), cancellation);
  }
}

Path
ModelStreamerTransferEngine::PrepareCheckpoint(const StorageBackend& destination, const Path& staging_directory,
    const std::string&, TransferMode mode, const PublishedArtifact* artifact, Cancellation cancellation) const
{
  if (!artifact || destination.has_filesystem())
    throw std::invalid_argument("S3 checkpoint requires an artifact destination");
  try {
    return ArtifactStorage().PrepareCheckpoint(*artifact, staging_directory, mode, cancellation);
  }
  catch (...) {
    RethrowS3TransferError(std::current_exception(), cancellation);
  }
}

void
ModelStreamerTransferEngine::ValidateCheckpointDestination(const StorageBackend& destination,
    const PublishedArtifact* artifact, Cancellation cancellation) const
{
  if (!artifact || destination.has_filesystem())
    throw std::invalid_argument("S3 checkpoint requires an artifact destination");
  try {
    ArtifactStorage().PrepareCheckpoint(*artifact, cancellation);
    cancellation.ThrowIfCancelled();
  }
  catch (...) {
    RethrowS3TransferError(std::current_exception(), cancellation);
  }
}

RestorePlan
ModelStreamerTransferEngine::InspectCheckpoint(const Path& source, const PublishedArtifact* artifact,
    Cancellation cancellation) const
{
  if (!artifact)
    throw std::invalid_argument("S3 checkpoint requires an artifact");
  try {
    ArtifactStorage().PrepareCheckpoint(*artifact, cancellation);
    return ArtifactStorage().InspectCheckpoint(source, *artifact, cancellation);
  }
  catch (...) {
    RethrowS3TransferError(std::current_exception(), cancellation);
  }
}

void
ModelStreamerTransferEngine::PublishCheckpoint(const Path& source, const StorageBackend& destination, RestorePlan plan,
    CheckpointPublication* publication, Cancellation cancellation, TransferMode mode) const
{
  if (!publication || destination.has_filesystem())
    throw std::invalid_argument("S3 checkpoint requires an artifact destination");
  try {
    if (mode == TransferMode::Direct)
      throw std::invalid_argument("S3 checkpoints require staging");
    ArtifactStorage().PublishCheckpoint(source, std::move(plan), *publication, cancellation);
  }
  catch (...) {
    RethrowS3TransferError(std::current_exception(), cancellation);
  }
}

}  // namespace snapshot::pagebroker

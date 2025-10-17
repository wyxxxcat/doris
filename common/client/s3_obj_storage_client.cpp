// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "s3_obj_storage_client.h"

#include <client/obj_storage_client.h>

namespace Aws::S3::Model {
class DeleteObjectRequest;
} // namespace Aws::S3::Model

using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris {
using namespace Aws::S3::Model;

// static constexpr int S3_REQUEST_THRESHOLD_MS = 5000;

ObjectStorageStatus s3fs_error(const Aws::S3::S3Error& err, std::string_view msg) {
    using namespace Aws::Http;
    switch (err.GetResponseCode()) {
    case HttpResponseCode::NOT_FOUND:
        return {TStatusCode::NOT_FOUND,
                fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    case HttpResponseCode::FORBIDDEN:
        // TODO: no permission and other 4xx errors should be handled separately
        return {-256, fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    default:
        return {TStatusCode::INTERNAL_ERROR,
                fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    }
}

ObjectStorageUploadResponse S3ObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts, const ObjectClientConfigOptions& config) {
    CreateMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetContentType("application/octet-stream");

    // MonotonicStopWatch watch;
    // watch.start();

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(_client->CreateMultipartUpload(request),
                                                "s3_file_writer::create_multi_part_upload",
                                                std::cref(request).get());
    SYNC_POINT_CALLBACK("s3_file_writer::_open", &outcome);
    // watch.stop();

    // s3_bvar::s3_multi_part_upload_latency << watch.elapsed_time_microseconds();
    // const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
    //                                              : outcome.GetError().GetRequestId();

    // LOG_IF(INFO, watch.elapsed_time_milliseconds() > S3_REQUEST_THRESHOLD_MS)
    //         << "CreateMultipartUpload cost=" << watch.elapsed_time_milliseconds() << "ms"
    //         << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key;

    if (!outcome.IsSuccess()) {
        auto st = s3fs_error(outcome.GetError(), fmt::format("failed to CreateMultipartUpload: {} ",
                                                             opts.full_path.native()));
        // LOG(WARNING) << st << " request_id=" << request_id;
        return ObjectStorageUploadResponse {
                .resp = {.status = st},
                .upload_id = std::nullopt,
                .etag = std::nullopt,
        };
    }

    return ObjectStorageUploadResponse {.resp = ObjectStorageResponse::OK(),
                                        .upload_id {outcome.GetResult().GetUploadId()}};
}

ObjectStorageResponse S3ObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                     std::string_view stream) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));
    request.SetBody(string_view_stream);
    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");

    // MonotonicStopWatch watch;
    // watch.start();
    auto outcome =
            SYNC_POINT_HOOK_RETURN_VALUE(_client->PutObject(request), "s3_file_writer::put_object",
                                         std::cref(request).get(), &stream);

    // watch.stop();

    // s3_bvar::s3_put_latency << watch.elapsed_time_microseconds();
    // const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
    //                                              : outcome.GetError().GetRequestId();

    if (!outcome.IsSuccess()) {
        auto st = s3fs_error(outcome.GetError(),
                             fmt::format("failed to put object: {}", opts.full_path.native()));
        // LOG(WARNING) << st << ", request_id=" << request_id;
        return ObjectStorageResponse {.status = st};
    }

    // LOG_IF(INFO, watch.elapsed_time_milliseconds() > S3_REQUEST_THRESHOLD_MS)
    //         << "PutObject cost=" << watch.elapsed_time_milliseconds() << "ms"
    //         << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key;
    return ObjectStorageResponse::OK();
}

ObjectStorageUploadResponse S3ObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                            std::string_view stream, int part_num) {
    UploadPartRequest request;
    request.WithBucket(opts.bucket)
            .WithKey(opts.key)
            .WithPartNumber(part_num)
            .WithUploadId(*opts.upload_id);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());

    request.SetBody(string_view_stream);

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");

    // MonotonicStopWatch watch;
    // watch.start();
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(_client->UploadPart(request),
                                                "s3_file_writer::upload_part",
                                                std::cref(request).get(), &stream);

    // watch.stop();

    // s3_bvar::s3_multi_part_upload_latency << watch.elapsed_time_microseconds();
    // const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
    //                                              : outcome.GetError().GetRequestId();

    TEST_SYNC_POINT_CALLBACK("S3FileWriter::_upload_one_part", &outcome);
    if (!outcome.IsSuccess()) {
        auto st = s3fs_error(outcome.GetError(),
                             fmt::format("failed to UploadPart: {}, part_num {}, upload_id={}",
                                         opts.full_path.native(), part_num, *opts.upload_id));

        // LOG(WARNING) << st << ", request_id=" << request_id;
        return ObjectStorageUploadResponse {.resp = {.status = st}};

        // LOG_IF(INFO, watch.elapsed_time_milliseconds() > S3_REQUEST_THRESHOLD_MS)
        //         << "UploadPart cost=" << watch.elapsed_time_milliseconds() << "ms"
        //         << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key
        //         << ", part_num=" << part_num << ", upload_id=" << *opts.upload_id;
        return ObjectStorageUploadResponse {.resp = ObjectStorageResponse::OK(),
                                            .etag = outcome.GetResult().GetETag()};
    }
    return ObjectStorageUploadResponse {.resp = ObjectStorageResponse::OK()};
}

ObjectStorageResponse S3ObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    CompleteMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(*opts.upload_id);

    CompletedMultipartUpload completed_upload;
    std::vector<CompletedPart> complete_parts;
    std::ranges::transform(completed_parts, std::back_inserter(complete_parts),
                           [](const ObjectCompleteMultiPart& part_ptr) {
                               CompletedPart part;
                               part.SetPartNumber(part_ptr.part_num);
                               part.SetETag(part_ptr.etag);
                               return part;
                           });
    completed_upload.SetParts(std::move(complete_parts));
    request.WithMultipartUpload(completed_upload);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3FileWriter::_complete:3", ObjectStorageResponse(), this);

    // MonotonicStopWatch watch;
    // watch.start();
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(_client->CompleteMultipartUpload(request),
                                                "s3_file_writer::complete_multi_part",
                                                std::cref(request).get());

    // watch.stop();
    // s3_bvar::s3_multi_part_upload_latency << watch.elapsed_time_microseconds();
    // const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
    //                                              : outcome.GetError().GetRequestId();

    if (!outcome.IsSuccess()) {
        auto st = s3fs_error(outcome.GetError(),
                             fmt::format("failed to CompleteMultipartUpload: {}, upload_id={}",
                                         opts.full_path.native(), *opts.upload_id));
        // LOG(WARNING) << st << ", request_id=" << request_id;
        return ObjectStorageResponse {.status = st};
    }

    // LOG_IF(INFO, watch.elapsed_time_milliseconds() > S3_REQUEST_THRESHOLD_MS)
    //         << "CompleteMultipartUpload cost=" << watch.elapsed_time_milliseconds() << "ms"
    //         << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key
    //         << ", upload_id=" << *opts.upload_id;
    return ObjectStorageResponse::OK();
}

ObjectStorageHeadResponse S3ObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);

    // SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            _client->HeadObject(request), "s3_file_system::head_object", std::ref(request).get());
    if (outcome.IsSuccess()) {
        return {.resp = ObjectStorageResponse::OK(),
                .file_size = outcome.GetResult().GetContentLength()};
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return {.resp = {TStatusCode::NOT_FOUND}};
    } else {
        return {.resp = {s3fs_error(outcome.GetError(), fmt::format("failed to head object: {}",
                                                                    opts.full_path.native())),
                         static_cast<int>(outcome.GetError().GetResponseCode()),
                         outcome.GetError().GetRequestId()}};
    }
    return ObjectStorageHeadResponse {
            .resp = ObjectStorageResponse::OK(),
    };
}

ObjectStorageResponse S3ObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                     void* buffer, size_t offset, size_t bytes_read,
                                                     size_t* size_return) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + bytes_read - 1));
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, bytes_read));

    // SCOPED_BVAR_LATENCY(s3_bvar::s3_get_latency);
    auto outcome = _client->GetObject(request);
    if (!outcome.IsSuccess()) {
        return ObjectStorageResponse {
                .status = s3fs_error(outcome.GetError(), fmt::format("failed to get object: {}",
                                                                     opts.full_path.native())),
        };
    }
    *size_return = outcome.GetResult().GetContentLength();
    if (*size_return != bytes_read) {
        return ObjectStorageResponse {
                .status = {TStatusCode::INTERNAL_ERROR,
                           fmt::format("incomplete read from {}, expect {}, got {}",
                                       opts.full_path.native(), bytes_read, *size_return)}};
    }
    return ObjectStorageResponse::OK();
}

std::unique_ptr<ObjectListIterator> S3ObjStorageClient::list_objects(
        const ObjectStoragePathOptions& opts, const ObjectClientConfigOptions& config) {
    return std::make_unique<S3ObjListIterator>(_client, opts.bucket, opts.key, config.endpoint);
}

ObjectStorageResponse S3ObjStorageClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                         std::vector<std::string> objs) {
    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(opts.bucket);
    Aws::S3::Model::Delete del;
    Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
    std::ranges::transform(objs, std::back_inserter(objects), [](auto&& obj_key) {
        Aws::S3::Model::ObjectIdentifier obj_identifier;
        obj_identifier.SetKey(std::move(obj_key));
        return obj_identifier;
    });
    del.WithObjects(std::move(objects)).SetQuiet(true);
    delete_request.SetDelete(std::move(del));
    // SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
    auto delete_outcome = _client->DeleteObjects(delete_request);
    if (!delete_outcome.IsSuccess()) {
        return ObjectStorageResponse {
                .status = s3fs_error(delete_outcome.GetError(),
                                     fmt::format("failed to delete dir {}", opts.key))};
    }
    if (!delete_outcome.GetResult().GetErrors().empty()) {
        const auto& e = delete_outcome.GetResult().GetErrors().front();
        return ObjectStorageResponse {
                .status = {TStatusCode::INTERNAL_ERROR,
                           fmt::format("failed to delete object {}: {}, request_id={}", opts.key,
                                       e.GetMessage(), delete_outcome.GetResult().GetRequestId())}};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageClient::delete_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);

    // SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_object_latency);
    auto outcome = _client->DeleteObject(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return ObjectStorageResponse::OK();
    }
    return ObjectStorageResponse {
            .status = {TStatusCode::INTERNAL_ERROR,
                       fmt::format("failed to delete object {}: {}, request_id={}", opts.key,
                                   outcome.GetError().GetMessage(),
                                   outcome.GetError().GetRequestId())}};
}

ObjectStorageResponse S3ObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts, const std::string& prefix) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket).WithPrefix(prefix);
    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(opts.bucket);
    bool is_trucated = false;
    do {
        Aws::S3::Model::ListObjectsV2Outcome outcome;
        {
            // SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            outcome = _client->ListObjectsV2(request);
        }
        if (!outcome.IsSuccess()) {
            return ObjectStorageResponse {
                    .status = {TStatusCode::INTERNAL_ERROR,
                               fmt::format("failed to list objects from {}: {}, request_id={}",
                                           prefix, outcome.GetError().GetMessage(),
                                           outcome.GetError().GetRequestId())}};
        }
        const auto& result = outcome.GetResult();
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            // SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
            auto delete_outcome = _client->DeleteObjects(delete_request);
            if (!delete_outcome.IsSuccess()) {
                return ObjectStorageResponse {
                        .status = {TStatusCode::INTERNAL_ERROR,
                                   fmt::format("failed to delete dir {}", opts.key)}};
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                return ObjectStorageResponse {
                        .status = {TStatusCode::INTERNAL_ERROR,
                                   fmt::format("failed to delete object {}: {}, request_id={}",
                                               opts.key, e.GetMessage(),
                                               delete_outcome.GetResult().GetRequestId())}};
            }
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return ObjectStorageResponse::OK();
}

std::string S3ObjStorageClient::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                       const ObjectClientConfigOptions& config,
                                                       int64_t expiration_secs) {
    return _client->GeneratePresignedUrl(opts.bucket, opts.key, Aws::Http::HttpMethod::HTTP_GET,
                                         expiration_secs);
}

ObjectStorageResponse S3ObjStorageClient::check_versioning(const std::string& endpoint_,
                                                           const std::string& bucket) {
    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(bucket);
    auto outcome = _client->GetBucketVersioning(request);

    if (outcome.IsSuccess()) {
        const auto& versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration != Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG(WARNING) << "Err for check interval: bucket doesn't enable bucket versioning"
                         << " endpoint=" << endpoint_ << " bucket=" << bucket;
            return ObjectStorageResponse {
                    .status =
                            s3fs_error(outcome.GetError(),
                                       fmt::format("failed to get bucket versioning: {}", bucket))};
        }
    } else {
        LOG(WARNING) << "Err for check interval: failed to get status of bucket versioning"
                     << " endpoint=" << endpoint_ << " bucket=" << bucket
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        return ObjectStorageResponse {.status = {-1}};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageClient::abort_multipart_upload(
        const ObjectStoragePathOptions& opts, const ObjectClientConfigOptions& config,
        const std::string& upload_id) {
    Aws::S3::Model::AbortMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(upload_id);
    auto outcome = _client->AbortMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        LOG(WARNING) << "failed to abort multipart upload"
                     << " endpoint=" << config.endpoint << " bucket=" << opts.bucket
                     << " key=" << opts.key << " upload_id=" << upload_id
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
            return {ObjectStorageResponse::OK()};
        }
        return ObjectStorageResponse {
                .status = {TStatusCode::INTERNAL_ERROR,
                           fmt::format("failed to abort multipart upload: {}, upload_id={}",
                                       opts.full_path.native(), upload_id)},
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId(),
        };
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageClient::get_life_cycle(const std::string& endpoint,
                                                         const std::string& bucket,
                                                         int64_t* expiration_days) {
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest request;
    request.SetBucket(bucket);

    auto outcome = _client->GetBucketLifecycleConfiguration(request);
    bool has_lifecycle = false;
    if (outcome.IsSuccess()) {
        const auto& rules = outcome.GetResult().GetRules();
        for (const auto& rule : rules) {
            if (rule.NoncurrentVersionExpirationHasBeenSet()) {
                has_lifecycle = true;
                *expiration_days = rule.GetNoncurrentVersionExpiration().GetNoncurrentDays();
            }
        }
    } else {
        LOG(WARNING) << "Err for check interval: failed to get bucket lifecycle"
                     << " endpoint=" << endpoint << " bucket=" << bucket
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        return ObjectStorageResponse {
                .status = s3fs_error(outcome.GetError(),
                                     fmt::format("failed to get lift cycle: {}", bucket))};
    }

    if (!has_lifecycle) {
        LOG(WARNING) << "Err for check interval: bucket doesn't have lifecycle configuration"
                     << " endpoint=" << endpoint << " bucket=" << bucket;
        return ObjectStorageResponse {.status = {-1}};
    }
    return ObjectStorageResponse::OK();
}

} // namespace doris
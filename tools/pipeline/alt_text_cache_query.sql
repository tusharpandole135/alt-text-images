-- Last 24h WebA11y alt text generation: Request joined with Job
-- Output: per-image row with base URL, context URL, existing alt text, cache hit
WITH
jobs AS (
  SELECT
    request_id,
    JSON_VALUE(meta_data, '$.feature_identifier.image_id') AS image_id,
    CAST(JSON_VALUE(response, '$.cacheHit') AS BOOL) AS cache_hit,
    JSON_VALUE(response, '$.altText') AS generated_alt_text,
    JSON_VALUE(response, '$.existingAltTextBucket') AS existing_alt_text_bucket,
    JSON_VALUE(response, '$.imageBucket') AS image_bucket,
    JSON_VALUE(response, '$.imageCategorisationConfidence') AS image_categorisation_confidence,
    CAST(JSON_VALUE(response, '$.isDecorative') AS BOOL) AS is_decorative,
    user_id,
    group_id,
    created_at AS job_created_at
  FROM `browserstack-production.tcg_service.tcg_llm_service_req_data_partitioned`
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    AND service = 'alttextGenerationWorker'
    AND JSON_VALUE(meta_data, '$.feature') = 'webAllyAlttextGeneration'
),
requests_unnested AS (
  SELECT
    request_id,
    JSON_VALUE(image, '$.id') AS image_id,
    JSON_VALUE(image, '$.baseImage') AS base_url,
    JSON_VALUE(image, '$.contextImage') AS context_url,
    JSON_VALUE(image, '$.altText') AS existing_alt_text,
    JSON_VALUE(image, '$.language') AS language,
    JSON_VALUE(image, '$.imageAltRes') AS image_alt_res
  FROM `browserstack-production.tcg_service.tcg_llm_service_req_data_partitioned`,
       UNNEST(JSON_QUERY_ARRAY(meta_data, '$.request_body.images')) AS image
  WHERE _PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    AND service = 'tcg'
    AND JSON_VALUE(meta_data, '$.feature') = 'webAllyAlttextGeneration'
)
SELECT
  j.request_id,
  j.image_id,
  r.base_url,
  r.context_url,
  r.existing_alt_text,
  j.cache_hit,
  j.generated_alt_text,
  j.existing_alt_text_bucket,
  j.image_bucket,
  j.image_categorisation_confidence,
  j.is_decorative,
  r.language,
  r.image_alt_res,
  j.user_id,
  j.group_id,
  j.job_created_at
FROM jobs j
LEFT JOIN requests_unnested r
  ON j.request_id = r.request_id AND j.image_id = r.image_id
ORDER BY j.job_created_at DESC

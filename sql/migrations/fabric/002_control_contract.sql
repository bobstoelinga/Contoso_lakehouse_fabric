CREATE TABLE IF NOT EXISTS {control}.metadata.project_release (
    project_id STRING NOT NULL, release_id STRING NOT NULL,
    contract_version STRING NOT NULL, document_json STRING NOT NULL,
    files_json STRING NOT NULL, status STRING NOT NULL,
    registered_at TIMESTAMP NOT NULL, validated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.control.metadata_pointer (
    project_id STRING NOT NULL, active_release_id STRING NOT NULL,
    generation BIGINT NOT NULL, updated_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.control.project_owner (
    project_id STRING NOT NULL, owner_id STRING, generation BIGINT NOT NULL,
    state STRING NOT NULL, heartbeat TIMESTAMP, job_instance_id STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.control.delivery (
    project_id STRING NOT NULL, source_system_id STRING NOT NULL,
    delivery_id STRING NOT NULL, delivery_sequence BIGINT NOT NULL,
    manifest_fingerprint STRING NOT NULL, scope_json STRING NOT NULL,
    status STRING NOT NULL, supersedes STRING, registered_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.control.batch_context (
    batch_id STRING NOT NULL, project_id STRING NOT NULL,
    source_system_id STRING NOT NULL, delivery_id STRING NOT NULL,
    delivery_sequence BIGINT NOT NULL, release_id STRING NOT NULL,
    load_date TIMESTAMP NOT NULL, registered_at TIMESTAMP NOT NULL,
    source_time TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.control.work_item (
    batch_id STRING NOT NULL, layer STRING NOT NULL, entity_id STRING NOT NULL,
    input_fingerprint STRING NOT NULL, status STRING NOT NULL,
    commit_id STRING, attempt INT NOT NULL, error_code STRING,
    updated_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.audit.events (
    event_id STRING NOT NULL, project_id STRING NOT NULL, batch_id STRING,
    correlation_id STRING NOT NULL, layer STRING, entity_id STRING,
    attempt INT NOT NULL, transition STRING NOT NULL,
    event_time TIMESTAMP NOT NULL, error_code STRING, context_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS {control}.control.commit_manifest (
    commit_id STRING NOT NULL, batch_id STRING NOT NULL,
    layer STRING NOT NULL, entity_id STRING NOT NULL,
    owner_generation BIGINT NOT NULL, input_fingerprint STRING NOT NULL,
    outputs_json STRING NOT NULL, status STRING NOT NULL,
    committed_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {gold}.publication.product_release (
    project_id STRING NOT NULL, product_id STRING NOT NULL, release_id STRING NOT NULL,
    input_commits_json STRING NOT NULL, entity_manifest_json STRING NOT NULL,
    cutoff TIMESTAMP NOT NULL, spark_validation STRING NOT NULL,
    sql_validation STRING NOT NULL, consumer_status STRING NOT NULL,
    status STRING NOT NULL, registered_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {gold}.publication.active_product (
    project_id STRING NOT NULL, product_id STRING NOT NULL,
    active_release_id STRING NOT NULL, previous_release_id STRING,
    generation BIGINT NOT NULL, updated_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS {gold}.publication.consumer_catalog (
    project_id STRING NOT NULL, product_id STRING NOT NULL,
    consumer_release_id STRING NOT NULL, model_id STRING NOT NULL,
    report_id STRING NOT NULL, report_url STRING NOT NULL,
    previous_consumer_release_id STRING, promotion_state STRING NOT NULL,
    updated_at TIMESTAMP NOT NULL
) USING DELTA;

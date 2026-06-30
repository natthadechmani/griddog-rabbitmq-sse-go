CREATE DATABASE IF NOT EXISTS appdb;
USE appdb;

CREATE TABLE IF NOT EXISTS message_logs (
  id             BIGINT NOT NULL AUTO_INCREMENT,
  flow           VARCHAR(32)  NOT NULL,
  correlation_id VARCHAR(64)  NOT NULL,
  service        VARCHAR(32)  NOT NULL,
  stage          VARCHAR(48)  NOT NULL,
  payload        JSON         NOT NULL,
  created_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_flow (flow),
  KEY idx_correlation (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

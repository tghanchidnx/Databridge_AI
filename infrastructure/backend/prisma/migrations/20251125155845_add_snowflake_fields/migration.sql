-- AlterTable
ALTER TABLE `connections` ADD COLUMN `access_expiry_time` DATETIME(3) NULL,
    ADD COLUMN `access_token` TEXT NULL,
    ADD COLUMN `last_tested_at` DATETIME(3) NULL,
    ADD COLUMN `refresh_expiry_time` DATETIME(3) NULL,
    ADD COLUMN `refresh_token` TEXT NULL,
    ADD COLUMN `server_type` VARCHAR(50) NOT NULL DEFAULT 'Snowflake',
    ADD COLUMN `snowflake_account` VARCHAR(255) NULL,
    ADD COLUMN `snowflake_client_id` TEXT NULL,
    ADD COLUMN `snowflake_client_secret` TEXT NULL,
    ADD COLUMN `snowflake_database` VARCHAR(255) NULL,
    ADD COLUMN `snowflake_role` VARCHAR(255) NULL,
    ADD COLUMN `snowflake_schema` VARCHAR(255) NULL,
    ADD COLUMN `snowflake_user` VARCHAR(255) NULL,
    ADD COLUMN `snowflake_warehouse` VARCHAR(255) NULL;

-- CreateIndex
CREATE INDEX `connections_snowflake_account_idx` ON `connections`(`snowflake_account`);

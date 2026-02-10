/*
  Warnings:

  - You are about to drop the column `created_by` on the `aliases` table. All the data in the column will be lost.
  - You are about to drop the column `access_expiry_time` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `access_token` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `database_name` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `refresh_expiry_time` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `refresh_token` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `schema_name` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `server_type` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `snowflake_account` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `snowflake_client_id` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `snowflake_client_secret` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `snowflake_pass` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `snowflake_user` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `snowflake_warehouse` on the `connections` table. All the data in the column will be lost.
  - You are about to drop the column `account_id_source` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `account_id_target` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `job_id` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `job_name` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `match_count` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `mismatch_count` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `progress` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `source_only_count` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `target_only_count` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `total_items` on the `schema_comparison_jobs` table. All the data in the column will be lost.
  - You are about to drop the column `connection_scope` on the `users` table. All the data in the column will be lost.
  - You are about to drop the column `user_scope` on the `users` table. All the data in the column will be lost.
  - Added the required column `user_id` to the `aliases` table without a default value. This is not possible if the table is not empty.
  - Added the required column `source_connection_id` to the `schema_comparison_jobs` table without a default value. This is not possible if the table is not empty.
  - Added the required column `source_database` to the `schema_comparison_jobs` table without a default value. This is not possible if the table is not empty.
  - Added the required column `source_schema` to the `schema_comparison_jobs` table without a default value. This is not possible if the table is not empty.
  - Added the required column `target_connection_id` to the `schema_comparison_jobs` table without a default value. This is not possible if the table is not empty.
  - Added the required column `target_database` to the `schema_comparison_jobs` table without a default value. This is not possible if the table is not empty.
  - Added the required column `target_schema` to the `schema_comparison_jobs` table without a default value. This is not possible if the table is not empty.

*/
-- DropIndex
DROP INDEX `connections_snowflake_account_idx` ON `connections`;

-- DropIndex
DROP INDEX `schema_comparison_jobs_job_id_idx` ON `schema_comparison_jobs`;

-- DropIndex
DROP INDEX `schema_comparison_jobs_job_id_key` ON `schema_comparison_jobs`;

-- DropIndex
DROP INDEX `server_integrations_is_default_idx` ON `server_integrations`;

-- AlterTable
ALTER TABLE `aliases` DROP COLUMN `created_by`,
    ADD COLUMN `user_id` VARCHAR(191) NOT NULL;

-- AlterTable
ALTER TABLE `connections` DROP COLUMN `access_expiry_time`,
    DROP COLUMN `access_token`,
    DROP COLUMN `database_name`,
    DROP COLUMN `refresh_expiry_time`,
    DROP COLUMN `refresh_token`,
    DROP COLUMN `schema_name`,
    DROP COLUMN `server_type`,
    DROP COLUMN `snowflake_account`,
    DROP COLUMN `snowflake_client_id`,
    DROP COLUMN `snowflake_client_secret`,
    DROP COLUMN `snowflake_pass`,
    DROP COLUMN `snowflake_user`,
    DROP COLUMN `snowflake_warehouse`,
    ADD COLUMN `credentials` TEXT NULL,
    ADD COLUMN `description` TEXT NULL;

-- AlterTable
ALTER TABLE `schema_comparison_jobs` DROP COLUMN `account_id_source`,
    DROP COLUMN `account_id_target`,
    DROP COLUMN `job_id`,
    DROP COLUMN `job_name`,
    DROP COLUMN `match_count`,
    DROP COLUMN `mismatch_count`,
    DROP COLUMN `progress`,
    DROP COLUMN `source_only_count`,
    DROP COLUMN `target_only_count`,
    DROP COLUMN `total_items`,
    ADD COLUMN `result` JSON NULL,
    ADD COLUMN `source_connection_id` VARCHAR(255) NOT NULL,
    ADD COLUMN `source_database` VARCHAR(255) NOT NULL,
    ADD COLUMN `source_schema` VARCHAR(255) NOT NULL,
    ADD COLUMN `target_connection_id` VARCHAR(255) NOT NULL,
    ADD COLUMN `target_database` VARCHAR(255) NOT NULL,
    ADD COLUMN `target_schema` VARCHAR(255) NOT NULL,
    MODIFY `status` VARCHAR(50) NOT NULL DEFAULT 'PENDING';

-- AlterTable
ALTER TABLE `server_integrations` ADD COLUMN `connection_id` VARCHAR(191) NULL;

-- AlterTable
ALTER TABLE `users` DROP COLUMN `connection_scope`,
    DROP COLUMN `user_scope`,
    ADD COLUMN `auth_type` VARCHAR(50) NOT NULL DEFAULT 'Microsoft SSO';

-- CreateIndex
CREATE INDEX `aliases_user_id_idx` ON `aliases`(`user_id`);

-- CreateIndex
CREATE INDEX `server_integrations_snowflake_account_idx` ON `server_integrations`(`snowflake_account`);

-- AddForeignKey
ALTER TABLE `server_integrations` ADD CONSTRAINT `server_integrations_connection_id_fkey` FOREIGN KEY (`connection_id`) REFERENCES `connections`(`id`) ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `schema_comparison_jobs` ADD CONSTRAINT `schema_comparison_jobs_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `aliases` ADD CONSTRAINT `aliases_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- CreateTable
CREATE TABLE `users` (
    `id` VARCHAR(191) NOT NULL,
    `user_name` VARCHAR(255) NOT NULL,
    `user_email` VARCHAR(255) NOT NULL,
    `password` VARCHAR(255) NULL,
    `access_token` TEXT NULL,
    `refresh_token` TEXT NULL,
    `access_expiry_time` DATETIME(3) NULL,
    `refresh_expiry_time` DATETIME(3) NULL,
    `connection_scope` VARCHAR(50) NOT NULL DEFAULT 'Microsoft',
    `user_scope` VARCHAR(50) NOT NULL DEFAULT 'Default',
    `is_active` BOOLEAN NOT NULL DEFAULT true,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    UNIQUE INDEX `users_user_email_key`(`user_email`),
    INDEX `users_user_email_idx`(`user_email`),
    INDEX `users_is_active_idx`(`is_active`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `connections` (
    `id` VARCHAR(191) NOT NULL,
    `user_id` VARCHAR(191) NOT NULL,
    `connection_name` VARCHAR(255) NOT NULL,
    `server_type` VARCHAR(50) NOT NULL,
    `connection_type` VARCHAR(50) NOT NULL,
    `access_token` TEXT NULL,
    `refresh_token` TEXT NULL,
    `access_expiry_time` DATETIME(3) NULL,
    `refresh_expiry_time` DATETIME(3) NULL,
    `snowflake_client_id` TEXT NULL,
    `snowflake_client_secret` TEXT NULL,
    `snowflake_user` VARCHAR(255) NULL,
    `snowflake_pass` TEXT NULL,
    `snowflake_account` VARCHAR(255) NOT NULL,
    `snowflake_warehouse` VARCHAR(255) NOT NULL,
    `database_name` VARCHAR(255) NOT NULL,
    `schema_name` VARCHAR(255) NOT NULL,
    `status` VARCHAR(50) NOT NULL DEFAULT 'active',
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `connections_user_id_idx`(`user_id`),
    INDEX `connections_snowflake_account_idx`(`snowflake_account`),
    INDEX `connections_status_idx`(`status`),
    UNIQUE INDEX `connections_user_id_connection_name_key`(`user_id`, `connection_name`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `server_integrations` (
    `id` VARCHAR(191) NOT NULL,
    `user_id` VARCHAR(191) NOT NULL,
    `server_type` VARCHAR(50) NOT NULL,
    `snowflake_account` VARCHAR(255) NOT NULL,
    `snowflake_client_id` TEXT NOT NULL,
    `snowflake_client_secret` TEXT NOT NULL,
    `is_default` BOOLEAN NOT NULL DEFAULT false,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `server_integrations_user_id_idx`(`user_id`),
    INDEX `server_integrations_is_default_idx`(`is_default`),
    UNIQUE INDEX `server_integrations_user_id_snowflake_account_key`(`user_id`, `snowflake_account`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `audit_logs` (
    `id` VARCHAR(191) NOT NULL,
    `user_id` VARCHAR(191) NOT NULL,
    `action` VARCHAR(255) NOT NULL,
    `entity` VARCHAR(255) NOT NULL,
    `entity_id` VARCHAR(255) NULL,
    `changes` JSON NULL,
    `ip_address` VARCHAR(45) NULL,
    `user_agent` TEXT NULL,
    `status` VARCHAR(50) NOT NULL,
    `error_msg` TEXT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `audit_logs_user_id_idx`(`user_id`),
    INDEX `audit_logs_action_idx`(`action`),
    INDEX `audit_logs_created_at_idx`(`created_at`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `schema_comparison_jobs` (
    `id` VARCHAR(191) NOT NULL,
    `job_id` VARCHAR(255) NOT NULL,
    `job_name` VARCHAR(255) NOT NULL,
    `user_id` VARCHAR(191) NOT NULL,
    `account_id_source` VARCHAR(255) NOT NULL,
    `account_id_target` VARCHAR(255) NOT NULL,
    `status` VARCHAR(50) NOT NULL DEFAULT 'pending',
    `progress` INTEGER NOT NULL DEFAULT 0,
    `total_items` INTEGER NOT NULL DEFAULT 0,
    `match_count` INTEGER NOT NULL DEFAULT 0,
    `mismatch_count` INTEGER NOT NULL DEFAULT 0,
    `source_only_count` INTEGER NOT NULL DEFAULT 0,
    `target_only_count` INTEGER NOT NULL DEFAULT 0,
    `started_at` DATETIME(3) NULL,
    `completed_at` DATETIME(3) NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    UNIQUE INDEX `schema_comparison_jobs_job_id_key`(`job_id`),
    INDEX `schema_comparison_jobs_user_id_idx`(`user_id`),
    INDEX `schema_comparison_jobs_job_id_idx`(`job_id`),
    INDEX `schema_comparison_jobs_status_idx`(`status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `aliases` (
    `id` VARCHAR(191) NOT NULL,
    `alias_type` VARCHAR(50) NOT NULL,
    `actual_name` VARCHAR(255) NOT NULL,
    `alias_name` VARCHAR(255) NOT NULL,
    `description` TEXT NULL,
    `created_by` VARCHAR(191) NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `aliases_alias_type_idx`(`alias_type`),
    INDEX `aliases_alias_name_idx`(`alias_name`),
    UNIQUE INDEX `aliases_alias_type_actual_name_key`(`alias_type`, `actual_name`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `connections` ADD CONSTRAINT `connections_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `server_integrations` ADD CONSTRAINT `server_integrations_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `audit_logs` ADD CONSTRAINT `audit_logs_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

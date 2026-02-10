-- AlterTable
ALTER TABLE `connections` ADD COLUMN `auth_type` VARCHAR(50) NULL,
    ADD COLUMN `database_name` VARCHAR(255) NULL,
    ADD COLUMN `host` VARCHAR(255) NULL,
    ADD COLUMN `port` INTEGER NULL,
    ADD COLUMN `schema_name` VARCHAR(255) NULL;

-- CreateTable
CREATE TABLE `comparison_resources` (
    `id` VARCHAR(191) NOT NULL,
    `job_id` VARCHAR(191) NOT NULL,
    `resource_name` VARCHAR(255) NOT NULL,
    `resource_type` VARCHAR(50) NOT NULL,
    `database` VARCHAR(255) NOT NULL,
    `schema` VARCHAR(255) NOT NULL,
    `status` VARCHAR(50) NOT NULL,
    `source_ddl` LONGTEXT NULL,
    `target_ddl` LONGTEXT NULL,
    `differences` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `comparison_resources_job_id_idx`(`job_id`),
    INDEX `comparison_resources_resource_name_idx`(`resource_name`),
    INDEX `comparison_resources_resource_type_idx`(`resource_type`),
    INDEX `comparison_resources_status_idx`(`status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `comparison_columns` (
    `id` VARCHAR(191) NOT NULL,
    `resource_id` VARCHAR(191) NOT NULL,
    `column_name` VARCHAR(255) NOT NULL,
    `status` VARCHAR(50) NOT NULL,
    `source_type` VARCHAR(100) NULL,
    `target_type` VARCHAR(100) NULL,
    `source_nullable` BOOLEAN NULL,
    `target_nullable` BOOLEAN NULL,
    `source_default` TEXT NULL,
    `target_default` TEXT NULL,
    `differences` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `comparison_columns_resource_id_idx`(`resource_id`),
    INDEX `comparison_columns_column_name_idx`(`column_name`),
    INDEX `comparison_columns_status_idx`(`status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `comparison_dependencies` (
    `id` VARCHAR(191) NOT NULL,
    `job_id` VARCHAR(191) NOT NULL,
    `source_resource_id` VARCHAR(191) NOT NULL,
    `target_resource_id` VARCHAR(191) NOT NULL,
    `dependency_type` VARCHAR(50) NOT NULL,
    `constraint_name` VARCHAR(255) NULL,
    `metadata` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `comparison_dependencies_job_id_idx`(`job_id`),
    INDEX `comparison_dependencies_source_resource_id_idx`(`source_resource_id`),
    INDEX `comparison_dependencies_target_resource_id_idx`(`target_resource_id`),
    INDEX `comparison_dependencies_dependency_type_idx`(`dependency_type`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `comparison_resources` ADD CONSTRAINT `comparison_resources_job_id_fkey` FOREIGN KEY (`job_id`) REFERENCES `schema_comparison_jobs`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `comparison_columns` ADD CONSTRAINT `comparison_columns_resource_id_fkey` FOREIGN KEY (`resource_id`) REFERENCES `comparison_resources`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `comparison_dependencies` ADD CONSTRAINT `comparison_dependencies_job_id_fkey` FOREIGN KEY (`job_id`) REFERENCES `schema_comparison_jobs`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `comparison_dependencies` ADD CONSTRAINT `comparison_dependencies_source_resource_id_fkey` FOREIGN KEY (`source_resource_id`) REFERENCES `comparison_resources`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `comparison_dependencies` ADD CONSTRAINT `comparison_dependencies_target_resource_id_fkey` FOREIGN KEY (`target_resource_id`) REFERENCES `comparison_resources`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

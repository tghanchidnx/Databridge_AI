/*
  Warnings:

  - You are about to drop the `smart_hierarchy_scripts` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE `smart_hierarchy_scripts` DROP FOREIGN KEY `smart_hierarchy_scripts_project_id_fkey`;

-- AlterTable
ALTER TABLE `hierarchy_projects` ADD COLUMN `deployment_config` JSON NULL;

-- DropTable
DROP TABLE `smart_hierarchy_scripts`;

-- CreateTable
CREATE TABLE `deployment_history` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `connection_id` VARCHAR(191) NOT NULL,
    `database` VARCHAR(255) NOT NULL,
    `schema` VARCHAR(255) NOT NULL,
    `master_table_name` VARCHAR(255) NOT NULL,
    `master_view_name` VARCHAR(255) NOT NULL,
    `database_type` VARCHAR(50) NOT NULL,
    `insert_script` LONGTEXT NULL,
    `view_script` LONGTEXT NULL,
    `mapping_script` LONGTEXT NULL,
    `dynamic_table_script` LONGTEXT NULL,
    `hierarchy_ids` JSON NOT NULL,
    `hierarchy_names` JSON NULL,
    `deployed_by` VARCHAR(255) NOT NULL,
    `deployed_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `status` VARCHAR(50) NOT NULL DEFAULT 'pending',
    `success_count` INTEGER NOT NULL DEFAULT 0,
    `failed_count` INTEGER NOT NULL DEFAULT 0,
    `error_message` TEXT NULL,
    `execution_time` INTEGER NULL,

    INDEX `deployment_history_project_id_idx`(`project_id`),
    INDEX `deployment_history_deployed_by_idx`(`deployed_by`),
    INDEX `deployment_history_deployed_at_idx`(`deployed_at`),
    INDEX `deployment_history_status_idx`(`status`),
    INDEX `deployment_history_database_type_idx`(`database_type`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `deployment_history` ADD CONSTRAINT `deployment_history_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

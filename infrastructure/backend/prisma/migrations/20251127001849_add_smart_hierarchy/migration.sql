-- AlterTable
ALTER TABLE `hierarchy_projects` ADD COLUMN `organization_id` VARCHAR(191) NULL;

-- CreateTable
CREATE TABLE `smart_hierarchy_master` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `hierarchy_id` VARCHAR(100) NOT NULL,
    `hierarchy_name` VARCHAR(255) NOT NULL,
    `description` TEXT NULL,
    `hierarchy_level` JSON NOT NULL,
    `flags` JSON NOT NULL,
    `mapping` JSON NOT NULL,
    `formula_config` JSON NULL,
    `filter_config` JSON NULL,
    `pivot_config` JSON NULL,
    `metadata` JSON NULL,
    `created_by` VARCHAR(191) NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `smart_hierarchy_master_project_id_idx`(`project_id`),
    INDEX `smart_hierarchy_master_hierarchy_id_idx`(`hierarchy_id`),
    UNIQUE INDEX `smart_hierarchy_master_project_id_hierarchy_id_key`(`project_id`, `hierarchy_id`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `smart_hierarchy_exports` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `export_name` VARCHAR(255) NOT NULL,
    `description` TEXT NULL,
    `export_data` JSON NOT NULL,
    `export_type` VARCHAR(50) NOT NULL DEFAULT 'manual',
    `version` VARCHAR(50) NULL,
    `created_by` VARCHAR(191) NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `smart_hierarchy_exports_project_id_idx`(`project_id`),
    INDEX `smart_hierarchy_exports_export_type_idx`(`export_type`),
    INDEX `smart_hierarchy_exports_created_at_idx`(`created_at`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `smart_hierarchy_scripts` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `hierarchy_id` VARCHAR(100) NOT NULL,
    `script_type` VARCHAR(50) NOT NULL,
    `script_content` LONGTEXT NOT NULL,
    `generated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `deployed_at` DATETIME(3) NULL,
    `deployment_status` VARCHAR(50) NULL DEFAULT 'pending',
    `error_message` TEXT NULL,

    INDEX `smart_hierarchy_scripts_project_id_idx`(`project_id`),
    INDEX `smart_hierarchy_scripts_hierarchy_id_idx`(`hierarchy_id`),
    INDEX `smart_hierarchy_scripts_script_type_idx`(`script_type`),
    INDEX `smart_hierarchy_scripts_deployment_status_idx`(`deployment_status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateIndex
CREATE INDEX `hierarchy_projects_organization_id_idx` ON `hierarchy_projects`(`organization_id`);

-- AddForeignKey
ALTER TABLE `hierarchy_projects` ADD CONSTRAINT `hierarchy_projects_organization_id_fkey` FOREIGN KEY (`organization_id`) REFERENCES `organizations`(`id`) ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `smart_hierarchy_master` ADD CONSTRAINT `smart_hierarchy_master_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `smart_hierarchy_exports` ADD CONSTRAINT `smart_hierarchy_exports_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `smart_hierarchy_scripts` ADD CONSTRAINT `smart_hierarchy_scripts_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

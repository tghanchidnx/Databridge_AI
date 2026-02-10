-- CreateTable
CREATE TABLE `hierarchy_projects` (
    `id` VARCHAR(191) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `description` TEXT NULL,
    `user_id` VARCHAR(191) NOT NULL,
    `is_active` BOOLEAN NOT NULL DEFAULT true,
    `metadata` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `hierarchy_projects_user_id_idx`(`user_id`),
    INDEX `hierarchy_projects_is_active_idx`(`is_active`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `hierarchy_versions` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `base_version_id` VARCHAR(191) NULL,
    `status` VARCHAR(50) NOT NULL DEFAULT 'draft',
    `approved_by` VARCHAR(191) NULL,
    `approved_at` DATETIME(3) NULL,
    `metadata` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `hierarchy_versions_project_id_idx`(`project_id`),
    INDEX `hierarchy_versions_status_idx`(`status`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `hierarchy_templates` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NULL,
    `name` VARCHAR(255) NOT NULL,
    `description` TEXT NULL,
    `nodes` JSON NOT NULL,
    `tags` JSON NULL,
    `is_global` BOOLEAN NOT NULL DEFAULT false,
    `created_by` VARCHAR(191) NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    INDEX `hierarchy_templates_project_id_idx`(`project_id`),
    INDEX `hierarchy_templates_is_global_idx`(`is_global`),
    INDEX `hierarchy_templates_created_by_idx`(`created_by`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `custom_objects` (
    `id` VARCHAR(191) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `data_type` VARCHAR(50) NOT NULL,
    `validation` JSON NULL,
    `metadata` JSON NULL,
    `created_by` VARCHAR(191) NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `custom_objects_created_by_idx`(`created_by`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `ai_recommendations` (
    `id` VARCHAR(191) NOT NULL,
    `type` VARCHAR(100) NOT NULL,
    `title` VARCHAR(500) NOT NULL,
    `description` TEXT NOT NULL,
    `evidence` JSON NULL,
    `status` VARCHAR(50) NOT NULL DEFAULT 'pending',
    `priority` INTEGER NOT NULL DEFAULT 1,
    `affectedNodes` JSON NULL,
    `brd_generated` BOOLEAN NOT NULL DEFAULT false,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `ai_recommendations_status_idx`(`status`),
    INDEX `ai_recommendations_priority_idx`(`priority`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CreateTable
CREATE TABLE `hierarchy_settings` (
    `id` VARCHAR(191) NOT NULL,
    `category` VARCHAR(100) NOT NULL,
    `key` VARCHAR(255) NOT NULL,
    `value` JSON NOT NULL,
    `updated_by` VARCHAR(191) NULL,
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `hierarchy_settings_category_idx`(`category`),
    UNIQUE INDEX `hierarchy_settings_category_key_key`(`category`, `key`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `hierarchy_projects` ADD CONSTRAINT `hierarchy_projects_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `hierarchy_versions` ADD CONSTRAINT `hierarchy_versions_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `hierarchy_templates` ADD CONSTRAINT `hierarchy_templates_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

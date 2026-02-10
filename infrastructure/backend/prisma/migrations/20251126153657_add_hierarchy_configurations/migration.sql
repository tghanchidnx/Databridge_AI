-- CreateTable
CREATE TABLE `hierarchy_configurations` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `type` VARCHAR(50) NOT NULL DEFAULT 'layout',
    `config` JSON NOT NULL,
    `is_default` BOOLEAN NOT NULL DEFAULT false,
    `created_by` VARCHAR(191) NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `hierarchy_configurations_project_id_idx`(`project_id`),
    INDEX `hierarchy_configurations_project_id_type_idx`(`project_id`, `type`),
    INDEX `hierarchy_configurations_is_default_idx`(`is_default`),
    UNIQUE INDEX `hierarchy_configurations_project_id_name_type_key`(`project_id`, `name`, `type`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `hierarchy_configurations` ADD CONSTRAINT `hierarchy_configurations_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

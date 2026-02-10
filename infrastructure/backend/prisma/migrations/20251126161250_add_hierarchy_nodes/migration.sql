-- CreateTable
CREATE TABLE `hierarchy_nodes` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `version_id` VARCHAR(191) NOT NULL DEFAULT 'main',
    `name` VARCHAR(255) NOT NULL,
    `parent_id` VARCHAR(191) NULL,
    `order` INTEGER NOT NULL DEFAULT 0,
    `node_type` VARCHAR(100) NULL,
    `description` TEXT NULL,
    `formula` TEXT NULL,
    `metadata` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `hierarchy_nodes_project_id_idx`(`project_id`),
    INDEX `hierarchy_nodes_version_id_idx`(`version_id`),
    INDEX `hierarchy_nodes_parent_id_idx`(`parent_id`),
    INDEX `hierarchy_nodes_project_id_version_id_idx`(`project_id`, `version_id`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `hierarchy_nodes` ADD CONSTRAINT `hierarchy_nodes_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

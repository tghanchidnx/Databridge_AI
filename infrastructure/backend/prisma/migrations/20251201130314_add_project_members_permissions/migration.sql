-- CreateTable
CREATE TABLE `hierarchy_project_members` (
    `id` VARCHAR(191) NOT NULL,
    `project_id` VARCHAR(191) NOT NULL,
    `user_id` VARCHAR(191) NULL,
    `user_email` VARCHAR(255) NULL,
    `role` VARCHAR(50) NOT NULL DEFAULT 'viewer',
    `access_type` VARCHAR(50) NOT NULL DEFAULT 'direct',
    `invited_by` VARCHAR(191) NULL,
    `invited_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `accepted_at` DATETIME(3) NULL,
    `is_active` BOOLEAN NOT NULL DEFAULT true,
    `metadata` JSON NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL,

    INDEX `hierarchy_project_members_project_id_idx`(`project_id`),
    INDEX `hierarchy_project_members_user_id_idx`(`user_id`),
    INDEX `hierarchy_project_members_user_email_idx`(`user_email`),
    INDEX `hierarchy_project_members_role_idx`(`role`),
    UNIQUE INDEX `hierarchy_project_members_project_id_user_id_key`(`project_id`, `user_id`),
    UNIQUE INDEX `hierarchy_project_members_project_id_user_email_key`(`project_id`, `user_email`),
    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- AddForeignKey
ALTER TABLE `hierarchy_project_members` ADD CONSTRAINT `hierarchy_project_members_project_id_fkey` FOREIGN KEY (`project_id`) REFERENCES `hierarchy_projects`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `hierarchy_project_members` ADD CONSTRAINT `hierarchy_project_members_user_id_fkey` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE `hierarchy_project_members` ADD CONSTRAINT `hierarchy_project_members_invited_by_fkey` FOREIGN KEY (`invited_by`) REFERENCES `users`(`id`) ON DELETE SET NULL ON UPDATE CASCADE;

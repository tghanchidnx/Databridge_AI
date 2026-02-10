-- AlterTable
ALTER TABLE `hierarchy_project_members` ADD COLUMN `invitation_status` VARCHAR(50) NOT NULL DEFAULT 'pending';

-- AlterTable
ALTER TABLE `organizations` ADD COLUMN `primary_use_case` VARCHAR(255) NULL,
    ADD COLUMN `team_size` VARCHAR(50) NULL;

-- CreateIndex
CREATE INDEX `hierarchy_project_members_invitation_status_idx` ON `hierarchy_project_members`(`invitation_status`);

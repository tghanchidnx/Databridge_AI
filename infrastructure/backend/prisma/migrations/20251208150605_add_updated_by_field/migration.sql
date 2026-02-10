-- AlterTable
ALTER TABLE `smart_hierarchy_master` ADD COLUMN `updated_by` VARCHAR(255) NULL,
    MODIFY `created_by` VARCHAR(255) NULL;

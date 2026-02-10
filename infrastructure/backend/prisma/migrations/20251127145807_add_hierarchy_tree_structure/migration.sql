-- AlterTable
ALTER TABLE `smart_hierarchy_master` ADD COLUMN `is_root` BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN `parent_id` VARCHAR(191) NULL,
    ADD COLUMN `sort_order` INTEGER NOT NULL DEFAULT 0;

-- CreateIndex
CREATE INDEX `smart_hierarchy_master_parent_id_idx` ON `smart_hierarchy_master`(`parent_id`);

-- CreateIndex
CREATE INDEX `smart_hierarchy_master_sort_order_idx` ON `smart_hierarchy_master`(`sort_order`);

-- CreateIndex
CREATE INDEX `smart_hierarchy_master_is_root_idx` ON `smart_hierarchy_master`(`is_root`);

-- AddForeignKey
ALTER TABLE `smart_hierarchy_master` ADD CONSTRAINT `smart_hierarchy_master_parent_id_fkey` FOREIGN KEY (`parent_id`) REFERENCES `smart_hierarchy_master`(`id`) ON DELETE CASCADE ON UPDATE CASCADE;

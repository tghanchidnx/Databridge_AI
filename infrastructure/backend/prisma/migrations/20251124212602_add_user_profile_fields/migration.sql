-- AlterTable
ALTER TABLE `users` ADD COLUMN `bio` TEXT NULL,
    ADD COLUMN `primary_use_case` VARCHAR(255) NULL,
    ADD COLUMN `team_size` VARCHAR(50) NULL;
